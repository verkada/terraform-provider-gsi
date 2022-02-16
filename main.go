package main

import (
	"terraform-provider-gsi/provider"

	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)


func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: provider.Provider,
	})
}
