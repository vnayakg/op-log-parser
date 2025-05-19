package services

import (
	"context"
	"log"
	"op-log-parser/application/ports"
	"op-log-parser/application/domain/services"
)

type OpLogProcessor struct {
	reader ports.Reader
	writer ports.Writer
	parser services.ParserService
}

func NewOpLogProcessor(reader ports.Reader, writer ports.Writer, parser services.ParserService) *OpLogProcessor {
	return &OpLogProcessor{
		reader: reader,
		writer: writer,
		parser: parser,
	}
}

func (p *OpLogProcessor) Process(ctx context.Context) error {
	oplogChan, errChan := p.reader.Read(ctx)
	processedChan := make(chan []string)

	go func() {
		defer close(processedChan)
		for oplog := range oplogChan {
			select {
			case <-ctx.Done():
				return
			default:
				processed, err := p.parser.Parse(oplog)
				if err != nil {
					log.Printf("Error processing oplog: %v\n", err)
					continue
				}
				processedChan <- processed
			}
		}
	}()

	writeErrChan := p.writer.Write(ctx, processedChan)

	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				errChan = nil
			} else if err != nil {
				log.Printf("Reader error: %v\n", err)
			}
		case err, ok := <-writeErrChan:
			if !ok {
				writeErrChan = nil
			} else if err != nil {
				log.Printf("Writer error: %v\n", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		if errChan == nil && writeErrChan == nil {
			return nil
		}
	}
}
