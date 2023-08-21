package main

import (
	"bufio"
	"log"
	"net/http"
	"strings"

	"github.com/r3labs/sse"
)

type streamingHelper struct {
	url          string
	eventChannel chan string
}

func NewStreamingHelper(url string, eventChannel chan string) *streamingHelper {
	return &streamingHelper{
		url:          url,
		eventChannel: eventChannel,
	}
}

func (s *streamingHelper) GetStreamingData() {
	resp, err := http.Get(s.url)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		event := scanner.Text()
		s.eventChannel <- event
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func (s *streamingHelper) GetStreamingDataWithCustomParse() {
	resp, err := http.Get(s.url)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	var eventLines []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			// Empty line indicates the end of an event
			event := strings.Join(eventLines, "\n")
			parsedData := parseEvent(event)
			s.eventChannel <- parsedData["data"]
			eventLines = nil
		} else {
			eventLines = append(eventLines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func (s *streamingHelper) GetServerSentStreamingData() {
	client := sse.NewClient(s.url)
	client.Subscribe("", func(msg *sse.Event) {
		// Got some data!
		s.eventChannel <- string(msg.Data)
	})
}

func parseEvent(event string) map[string]string {
	lines := strings.Split(event, "\n")
	data := make(map[string]string)

	for _, line := range lines {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			data[key] = value
		}
	}
	return data
}
