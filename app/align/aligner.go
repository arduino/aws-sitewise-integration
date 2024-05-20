package align

import (
	"context"
	"fmt"

	"github.com/arduino/aws-sitewise-integration/business/entityalign"
	"github.com/arduino/aws-sitewise-integration/business/tsalign"
	"github.com/arduino/aws-sitewise-integration/internal/iot"
	"github.com/arduino/aws-sitewise-integration/internal/sitewiseclient"
	"github.com/arduino/aws-sitewise-integration/internal/utils"
	iotclient "github.com/arduino/iot-client-go"
	"github.com/sirupsen/logrus"
)

func StartAlignAndImport(ctx context.Context, logger *logrus.Entry, key, secret, orgid string, tagsF *string, alignEntities bool, resolution, timeWindowMinutes int) error {

	if (timeWindowMinutes*60)/resolution > 10 {
		return fmt.Errorf("timeWindowMinutes/resolution must be less or equal to 10")
	}

	// Init clients
	sitewisecl, err := sitewiseclient.New()
	if err != nil {
		return err
	}
	iotcl, err := iot.NewClient(key, secret, orgid)
	if err != nil {
		return err
	}

	if tagsF == nil {
		logger.Infoln("Things - searching with no filter")
	} else {
		logger.Infoln("Things - searching by tags: ", *tagsF)
	}
	things, err := iotcl.ThingList(ctx, nil, nil, true, utils.ParseTags(tagsF))
	if err != nil {
		return err
	}
	thingsMap := make(map[string]iotclient.ArduinoThing, len(things))
	for _, thing := range things {
		logger.Infoln("  Thing: ", thing.Id, thing.Name)
		thingsMap[thing.Id] = thing
	}

	tsAlignerClient := tsalign.New(sitewisecl, iotcl, logger)

	if alignEntities {
		err = entityalign.Align(ctx, things, sitewisecl)
		if err != nil {
			return err
		}
	}

	// Extract data points from thing and push to SiteWise
	if err := tsAlignerClient.AlignTimeSeriesSamplesIntoSiteWise(ctx, timeWindowMinutes, thingsMap, resolution); err != nil {
		logger.Error("Error aligning time series samples: ", err)
	}

	return nil
}
