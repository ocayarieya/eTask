package workflow

import (
	"github.com/KKKKjl/eTask/message"
	"github.com/KKKKjl/eTask/utils"
)

// Pipeline represents a pipeline of jobs.
type Pipeliner struct {
	jobs []*message.Message
}

func NewPipeliner(jobs ...*message.Message) Pipeliner {
	for _, job := range jobs {
		if job.ID == "" {
			job.ID = utils.GetUUID()
		}
	}

	for i := len(jobs) - 1; i > 0; i-- {
		jobs[i-1].NextJobId = jobs[i].ID
	}

	return Pipeliner{
		jobs: jobs,
	}
}

func (p *Pipeliner) Jobs() []*message.Message {
	return p.jobs
}
