package parser

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

type stubExchange struct{}

func (s stubExchange) ExchangePageUrl(from, to string) string {
	return ""
}

func (s stubExchange) Selector() NodeSelector {
	return NodeSelector{
		Tag:  atom.Td,
		Attr: "class",
		Val:  "rightCol",
		Pos:  0,
	}
}

func (s stubExchange) FormatRate(rate string) (float64, error) {
	return strconv.ParseFloat(strings.Trim(rate, "\u00a0"), 64)
}

var (
	dom         *html.Node
	nodesToFind = NodeSelector{
		Tag:  atom.Td,
		Attr: "class",
		Val:  "rightCol",
	}

	stubXEPage = `
<html>
<body>
<table>
<tr class="uccRes">Something here
<td class="rightCol">10.23&nbsp</td>
<td class="rightCol">1</td>
</tr>
</table>
<div class="rightCol">11</div>
</body>
</html>`
)

func TestGetRate(t *testing.T) {
	// redefine get
	get = func(string) (*http.Response, error) {
		var r http.Response
		r.Body = ioutil.NopCloser(strings.NewReader(stubXEPage))
		return &r, nil
	}

	f, err := GetRate(stubExchange{}, "SMT", "SMTELSE")
	if err != nil {
		t.Error(err)
		return
	}

	if 10.23 != f {
		t.Errorf("want %f, got %f", 10.23, f)
	}
}

func TestFindNodes(t *testing.T) {

	nodes := findNodes(dom, nodesToFind)

	if len(nodes) != 2 {
		t.Errorf("want nodes %d, got %d", 2, len(nodes))
	}
}

func TestGetText(t *testing.T) {
	nodes := findNodes(dom, nodesToFind)

	if getText(nodes[1]) != "1" {
		t.Errorf("want text in node %s, got %s", "1", getText(nodes[1]))
	}
}

func TestMain(m *testing.M) {
	var err error
	dom, err = html.Parse(strings.NewReader(stubXEPage))
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(m.Run())
}
