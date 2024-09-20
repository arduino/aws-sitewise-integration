// This file is part of arduino aws-sitewise-integration.
//
// Copyright 2024 ARDUINO SA (http://www.arduino.cc/)
//
// This software is released under the Mozilla Public License Version 2.0,
// which covers the main part of aws-sitewise-integration.
// The terms of this license can be found at:
// https://www.mozilla.org/media/MPL/2.0/index.815ca599c9df.txt
//
// You can be released from the requirements of the above licenses by purchasing
// a commercial license. Buying such a license is mandatory if you want to
// modify or otherwise use the software for commercial activities involving the
// Arduino software without disclosing the source code of your own applications.
// To purchase a commercial license, send an email to license@arduino.cc.

package align

import (
	"context"

	"github.com/arduino/aws-sitewise-integration/business/entityalign"
	"github.com/arduino/aws-sitewise-integration/business/tsalign"
	"github.com/arduino/aws-sitewise-integration/internal/iot"
	"github.com/arduino/aws-sitewise-integration/internal/sitewiseclient"
	"github.com/arduino/aws-sitewise-integration/internal/utils"
	iotclient "github.com/arduino/iot-client-go/v2"
	"github.com/sirupsen/logrus"
)

type entityAligner struct {
	logger     *logrus.Entry
	sitewisecl *sitewiseclient.IotSiteWiseClient
	iotcl      *iot.Client
}

func New(key, secret, orgid string, logger *logrus.Entry) (*entityAligner, []error) {
	// Init clients
	sitewisecl, err := sitewiseclient.New(logger)
	if err != nil {
		return nil, []error{err}
	}
	iotcl, err := iot.NewClient(key, secret, orgid)
	if err != nil {
		return nil, []error{err}
	}

	return &entityAligner{
		logger:     logger,
		sitewisecl: sitewisecl,
		iotcl:      iotcl,
	}, nil
}

func (a *entityAligner) StartAlignAndImport(ctx context.Context, tagsF *string, alignEntities bool, resolution, timeWindowMinutes int) []error {
	if tagsF == nil {
		a.logger.Infoln("Things - searching with no filter")
	} else {
		a.logger.Infoln("Things - searching by tags: ", *tagsF)
	}
	things, err := a.iotcl.ThingList(ctx, nil, nil, true, utils.ParseTags(tagsF))
	if err != nil {
		return []error{err}
	}
	thingsMap := make(map[string]iotclient.ArduinoThing, len(things))
	for _, thing := range things {
		a.logger.Infoln("  Thing: ", thing.Id, thing.Name)
		thingsMap[thing.Id] = thing
	}

	if alignEntities {
		propertyDefintions, err := a.iotcl.PropertiesDefinition(ctx)
		if err != nil {
			return []error{err}
		}
		a.logger.Debugln("Loaded # properties definition: ", len(propertyDefintions))
		aligner := entityalign.New(a.sitewisecl, a.logger)
		errs := aligner.Align(ctx, things, propertyDefintions)
		if errs != nil {
			return errs
		}
	}

	// Extract data points from thing and push to SiteWise
	tsAlignerClient := tsalign.New(a.sitewisecl, a.iotcl, a.logger)
	if err := tsAlignerClient.AlignTimeSeriesSamplesIntoSiteWise(ctx, timeWindowMinutes, thingsMap, resolution); err != nil {
		return err
	}

	return nil
}
