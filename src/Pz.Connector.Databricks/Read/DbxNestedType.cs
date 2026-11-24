using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.Databricks;

/// <summary>Reads a Databricks <c>ARRAY&lt;T&gt;</c>/<c>MAP&lt;K,V&gt;</c>/<c>STRUCT&lt;...&gt;</c>
/// <c>type_text</c> into the Arrow list, map and struct whose leaves are the flat types the engine
/// carries: int32, int64, double, decimal128, utf8, bool, date32 and timestamp (microseconds, UTC).
/// A type with any leaf outside that set (binary, <c>TIMESTAMP_NTZ</c>, interval, variant, a nested
/// map key) has no engine representation, so the whole column stays JSON text; nothing is
/// approximated. Narrow integers widen to int32 and <c>FLOAT</c> to double, which JSON text carries
/// exactly.</summary>
internal sealed class DbxNestedType
{
    private readonly string _text;
    private int _at;

    private DbxNestedType(string text) => _text = text;

    /// <summary>The Arrow type of a nested <paramref name="typeText"/>, or <see langword="null"/> when the
    /// text is not a nested type or cannot be fully mapped.</summary>
    public static IArrowType? TryParse(string typeText)
    {
        var head = typeText.TrimStart();
        if (!(head.StartsWith("ARRAY", StringComparison.OrdinalIgnoreCase)
            || head.StartsWith("MAP", StringComparison.OrdinalIgnoreCase)
            || head.StartsWith("STRUCT", StringComparison.OrdinalIgnoreCase)))
        {
            return null;
        }

        var parser = new DbxNestedType(typeText);
        var type = parser.Type();
        parser.Space();
        return type is not null && parser._at == typeText.Length ? type : null;
    }

    private IArrowType? Type()
    {
        Space();
        var word = Word();
        IArrowType? type;
        switch (word)
        {
            case "ARRAY":
                type = Expect('<') ? Array() : null;
                break;
            case "MAP":
                type = Expect('<') ? Map() : null;
                break;
            case "STRUCT":
                type = Expect('<') ? Struct() : null;
                break;
            default:
                type = Leaf(word);
                break;
        }

        SkipNotNull();
        return type;
    }

    private IArrowType? Array()
    {
        var value = Type();
        return value is not null && Expect('>') ? new ListType(new Field("item", value, true)) : null;
    }

    private IArrowType? Map()
    {
        var key = Type();
        if (key is null or NestedType || !Expect(','))
        {
            return null;
        }

        var value = Type();
        return value is not null && Expect('>') ? new MapType(key, value) : null;
    }

    private IArrowType? Struct()
    {
        var fields = new List<Field>();
        while (true)
        {
            Space();
            var name = Name();
            if (name is null || !Expect(':'))
            {
                return null;
            }

            var type = Type();
            if (type is null)
            {
                return null;
            }

            SkipComment();
            fields.Add(new Field(name, type, true));
            Space();
            if (Peek() == ',')
            {
                _at++;
                continue;
            }

            return Expect('>') && fields.Count > 0 ? new StructType(fields) : null;
        }
    }

    private IArrowType? Leaf(string word)
    {
        switch (word)
        {
            case "INT" or "INTEGER" or "SMALLINT" or "SHORT" or "TINYINT" or "BYTE": return Int32Type.Default;
            case "BIGINT" or "LONG": return Int64Type.Default;
            case "FLOAT" or "REAL" or "DOUBLE": return DoubleType.Default;
            case "STRING" or "VARCHAR" or "CHAR":
                SkipParenthesized();
                return StringType.Default;
            case "BOOLEAN": return BooleanType.Default;
            case "DATE": return Date32Type.Default;
            case "TIMESTAMP": return new TimestampType(TimeUnit.Microsecond, "Etc/UTC");
            case "DECIMAL" or "DEC" or "NUMERIC":
                var precision = 10;
                var scale = 0;
                if (Peek() == '(')
                {
                    _at++;
                    precision = Number();
                    Space();
                    if (Peek() == ',')
                    {
                        _at++;
                        scale = Number();
                    }

                    if (!Expect(')'))
                    {
                        return null;
                    }
                }

                return new Decimal128Type(precision, scale);
            default:
                return null;
        }
    }

    private int Number()
    {
        Space();
        var start = _at;
        while (_at < _text.Length && char.IsAsciiDigit(_text[_at]))
        {
            _at++;
        }

        return int.Parse(_text.AsSpan(start, _at - start), CultureInfo.InvariantCulture);
    }

    private string Word()
    {
        Space();
        var start = _at;
        while (_at < _text.Length && (char.IsAsciiLetterOrDigit(_text[_at]) || _text[_at] == '_'))
        {
            _at++;
        }

        return _text[start.._at].ToUpperInvariant();
    }

    /// <summary>A struct field name: bare, or backtick-quoted (<c>``</c> for a backtick).</summary>
    private string? Name()
    {
        Space();
        if (Peek() == '`')
        {
            _at++;
            var name = new System.Text.StringBuilder();
            while (_at < _text.Length)
            {
                if (_text[_at] == '`')
                {
                    if (_at + 1 < _text.Length && _text[_at + 1] == '`')
                    {
                        name.Append('`');
                        _at += 2;
                        continue;
                    }

                    _at++;
                    return name.ToString();
                }

                name.Append(_text[_at++]);
            }

            return null;
        }

        var start = _at;
        while (_at < _text.Length && (char.IsLetterOrDigit(_text[_at]) || _text[_at] == '_'))
        {
            _at++;
        }

        return _at > start ? _text[start.._at] : null;
    }

    private void SkipNotNull()
    {
        Space();
        if (string.Compare(_text, _at, "NOT NULL", 0, 8, StringComparison.OrdinalIgnoreCase) == 0)
        {
            _at += 8;
        }
    }

    /// <summary>A struct field's <c>COMMENT '...'</c> (<c>''</c> or <c>\'</c> inside the literal).</summary>
    private void SkipComment()
    {
        Space();
        if (string.Compare(_text, _at, "COMMENT", 0, 7, StringComparison.OrdinalIgnoreCase) != 0)
        {
            return;
        }

        _at += 7;
        Space();
        if (Peek() != '\'')
        {
            return;
        }

        _at++;
        while (_at < _text.Length)
        {
            if (_text[_at] == '\\')
            {
                _at += 2;
            }
            else if (_text[_at] == '\'')
            {
                _at++;
                if (Peek() == '\'')
                {
                    _at++;
                    continue;
                }

                return;
            }
            else
            {
                _at++;
            }
        }
    }

    private void SkipParenthesized()
    {
        if (Peek() == '(')
        {
            while (_at < _text.Length && _text[_at] != ')')
            {
                _at++;
            }

            _at++;
        }
    }

    private bool Expect(char c)
    {
        Space();
        if (Peek() != c)
        {
            return false;
        }

        _at++;
        return true;
    }

    private char Peek() => _at < _text.Length ? _text[_at] : '\0';

    private void Space()
    {
        while (_at < _text.Length && char.IsWhiteSpace(_text[_at]))
        {
            _at++;
        }
    }
}
