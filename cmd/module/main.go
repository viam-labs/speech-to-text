package main

import (
	"speechtotext/elevenlabs"
	"speechtotext/google"
	"speechtotext/utils"

	generic "go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/module"
	"go.viam.com/rdk/resource"
)

func main() {
	// ModularMain can take multiple APIModel arguments, if your module implements multiple models.
	module.ModularMain(
		resource.APIModel{API: generic.API, Model: google.Model},
		resource.APIModel{API: generic.API, Model: elevenlabs.Model},
		resource.APIModel{API: sensor.API, Model: utils.SessionSensor},
	)
}
