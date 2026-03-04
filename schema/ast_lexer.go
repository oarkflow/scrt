package schema

import (
	"strings"
	"unicode"
)

type ASTTokenType int

const (
	ASTTokenEOF ASTTokenType = iota
	ASTTokenAt
	ASTTokenColon
	ASTTokenComma
	ASTTokenLParen
	ASTTokenRParen
	ASTTokenDot
	ASTTokenArrow
	ASTTokenEquals
	ASTTokenWord
	ASTTokenString
)

type ASTToken struct {
	Type    ASTTokenType
	Literal string
}

type ASTLexer struct {
	input []rune
	pos   int
}

func NewASTLexer(input string) *ASTLexer {
	return &ASTLexer{input: []rune(input)}
}

func (l *ASTLexer) Next() ASTToken {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return ASTToken{Type: ASTTokenEOF}
	}
	ch := l.input[l.pos]
	switch ch {
	case '@':
		l.pos++
		return ASTToken{Type: ASTTokenAt, Literal: "@"}
	case ':':
		l.pos++
		return ASTToken{Type: ASTTokenColon, Literal: ":"}
	case ',':
		l.pos++
		return ASTToken{Type: ASTTokenComma, Literal: ","}
	case '(':
		l.pos++
		return ASTToken{Type: ASTTokenLParen, Literal: "("}
	case ')':
		l.pos++
		return ASTToken{Type: ASTTokenRParen, Literal: ")"}
	case '.':
		l.pos++
		return ASTToken{Type: ASTTokenDot, Literal: "."}
	case '=':
		l.pos++
		return ASTToken{Type: ASTTokenEquals, Literal: "="}
	case '-':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '>' {
			l.pos += 2
			return ASTToken{Type: ASTTokenArrow, Literal: "->"}
		}
		return l.readWord()
	case '"', '\'', '`':
		return l.readString(ch)
	default:
		return l.readWord()
	}
}

func (l *ASTLexer) readString(quote rune) ASTToken {
	start := l.pos
	l.pos++
	for l.pos < len(l.input) {
		if l.input[l.pos] == quote {
			l.pos++
			break
		}
		l.pos++
	}
	return ASTToken{Type: ASTTokenString, Literal: string(l.input[start:l.pos])}
}

func (l *ASTLexer) readWord() ASTToken {
	start := l.pos
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsSpace(ch) || strings.ContainsRune("@:,().=>", ch) {
			break
		}
		if ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '>' {
			break
		}
		l.pos++
	}
	if start == l.pos {
		l.pos++
		return l.Next()
	}
	return ASTToken{Type: ASTTokenWord, Literal: string(l.input[start:l.pos])}
}

func (l *ASTLexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) {
		l.pos++
	}
}

func LexLine(line string) []ASTToken {
	lex := NewASTLexer(line)
	out := make([]ASTToken, 0, 16)
	for {
		tok := lex.Next()
		out = append(out, tok)
		if tok.Type == ASTTokenEOF {
			break
		}
	}
	return out
}
