package parser

import (
	"errors"
	"net/http"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

var (
	get = http.Get
)

// ExchangeSource defines source to use in parser
type ExchangeSource interface {
	ExchangePageUrl(from, to string) string
	// returns node selector where data located
	Selector() NodeSelector
	FormatRate(rate string) (float64, error)
}

// NodeSelector node description to find in dom tree
type NodeSelector struct {
	Tag  atom.Atom
	Attr string
	Val  string
	Pos  int
}

// GetRate make http get request to requried page and parse it to find node
// according to selector
func GetRate(src ExchangeSource, from, to string) (float64, error) {
	var rate float64
	// get html page
	r, err := get(src.ExchangePageUrl(from, to))
	if err != nil {
		return rate, err
	}
	// find core element to parse
	defer r.Body.Close()
	// generate dom tree
	d, err := html.Parse(r.Body)
	if err != nil {
		return rate, err
	}

	// find nodes according to selector
	nodes := findNodes(d, src.Selector())
	if len(nodes) < src.Selector().Pos {
		return rate, errors.New("html page parse err node position wrong")
	}
	//TODO does not correctly handle financial number format like 2,923.12
	rate, err = src.FormatRate(getText(nodes[src.Selector().Pos]))
	if err != nil {
		return rate, errors.New(err.Error() + "From: " + from + " To: " + to)
	}
	return rate, nil
}

// get text from node
func getText(n *html.Node) string {
	var s string
	// simplest will be
	if n.FirstChild != nil {
		s = n.FirstChild.Data
	}
	return s
}

// traverse dom tree recursively to find node by selector
func findNodes(h *html.Node, s NodeSelector) []*html.Node {
	nodes := make([]*html.Node, 0)
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.DataAtom == s.Tag {
			// if attr given search through attr values
			if s.Attr != "" {
				for _, a := range n.Attr {
					if a.Key == s.Attr && a.Val == s.Val {
						// save pointer to nodes
						nodes = append(nodes, n)
					}
				}
			} else {
				nodes = append(nodes, n)
			}

		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(h)
	return nodes
}
