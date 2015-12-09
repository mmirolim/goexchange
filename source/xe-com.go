package source

import (
	"strconv"
	"strings"

	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/parser"

	"golang.org/x/net/html/atom"
)

const (
	XE_COM XE = "XE-COM"
)

type XE string

func (XE) ExchangePageUrl(from, to string) string {
	return "http://www.xe.com/currencyconverter/convert/?Amount=1&From=" + from + "&To=" + to + "&r=#converter"
}

func (XE) Selector() parser.NodeSelector {
	return parser.NodeSelector{
		Tag:  atom.Td,
		Attr: "class",
		Val:  "rightCol",
		Pos:  0,
	}
}

func (XE) FormatRate(rate string) (float64, error) {
	return strconv.ParseFloat(strings.Trim(rate, "\u00a0"), 64)
}
