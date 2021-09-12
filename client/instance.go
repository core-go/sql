package client

import (
	"net/http"
)

type ProxyClient struct {
	Client *http.Client
	Url    string
}
