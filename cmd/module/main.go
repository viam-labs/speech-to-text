package main

import (
	"speechtotext"

	generic "go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/module"
	"go.viam.com/rdk/resource"
)

func main() {
	// ModularMain can take multiple APIModel arguments, if your module implements multiple models.
	module.ModularMain(
		resource.APIModel{API: generic.API, Model: speechtotext.GoogleCloudSTT},
		resource.APIModel{API: sensor.API, Model: speechtotext.SessionSensor},
	)
}
