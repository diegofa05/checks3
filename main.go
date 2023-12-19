package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	bucket  = "kube-metrics-thanos"
	region  = "us-east-1"
	profile = "default"
)

var startDate string
var endDate string
var log = logrus.New()

func init() {
	flag.StringVar(&startDate, "start", "", "Start date")
	flag.StringVar(&endDate, "end", "", "End date")
	flag.Parse()
}

var clustersList = []string{
	"use1-eks23-core-job-01",
	"use1-eks23-test-01",
	"use1-eks23-test-bq-02"
}

var metricsList = []string{
	"cpu-limits",
	"cpu-requests",
	"cpu-throttled-percentage",
	"cpu-usage",
}

func getDates(date1, date2 string) ([]string, error) {
	start, err := time.Parse("2006-01-02", date1)
	if err != nil {
		return nil, err
	}
	end, err := time.Parse("2006-01-02", date2)
	if err != nil {
		return nil, err
	}

	listDates := []string{}
	for d := start; d.Before(end) || d.Equal(end); d = d.AddDate(0, 0, 1) {
		for hour := 0; hour < 24; hour++ {
			for _, metricsList := range metricsList {
				sdate := metricsList + "/" + d.Add(time.Duration(hour)*time.Hour).Format("2006/01/02/15:04:05")
				listDates = append(listDates, strings.Split(sdate, ":")[0])
			}
		}
	}
	return listDates, nil
}

func printErros(errs []string) {
	if len(errs) == 0 {
		log.Info("No errors found")
		return
	}
	for _, err := range errs {
		info := strings.Split(err, "/")
		prnt := fmt.Sprintf("Missing info for clusters: %s metric: %s at date: %s-%s-%s %s:00:00",
			info[5],
			info[0],
			info[1],
			info[2],
			info[3],
			info[4],
		)

		log.Info(prnt)
	}
}

func main() {
	log.Out = os.Stdout
	// You could set this to any `io.Writer` such as a file
	file, err := os.OpenFile("logrus.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		log.Out = file
	} else {
		log.Info("Failed to log to file, using default stderr")
	}

	log.Info("Starting...")

	names, err := getDates(startDate, endDate)
	if err != nil {
		log.Error(fmt.Sprintf("Error parsing dates start [%s] or end [%s]: ", startDate, endDate), err)
		panic("Error parsing dates")
	}

	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithSharedConfigProfile(profile),
		config.WithRegion(region))
	if err != nil {
		fmt.Println("Error en credenciales.")
		return
	}
	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	listErr := []string{}
	for _, prefix := range names {
		output, err := client.ListObjectsV2(context.TODO(),
			&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)},
		)
		if err != nil {
			log.Error("Couldn't list objects for your account. Here's why: ", err)
			return
		}

		for _, cluster := range clustersList {
			rst := 0
			for _, object := range output.Contents {
				s3obj := aws.ToString(object.Key)
				if strings.Contains(s3obj, cluster) {
					rst = 1
					break
				}
			}
			if rst == 0 {
				listErr = append(listErr, prefix+"/"+cluster)
			}
		}
	}
	printErros(listErr)
}
