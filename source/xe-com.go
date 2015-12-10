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

// Define xe-com source
type XE string

// ExchangePageUrl returns proper url for a source where required data is
func (XE) ExchangePageUrl(from, to string) string {
	return "http://www.xe.com/currencyconverter/convert/?Amount=1&From=" + from + "&To=" + to + "&r=#converter"
}

// Selector defines where exchange rate node placed
func (XE) Selector() parser.NodeSelector {
	return parser.NodeSelector{
		Tag:  atom.Td,
		Attr: "class",
		Val:  "rightCol",
		Pos:  0,
	}
}

// FormatRate parse exchange rate text in source to float64
func (XE) FormatRate(rate string) (float64, error) {
	return strconv.ParseFloat(strings.Trim(rate, "\u00a0"), 64)
}
