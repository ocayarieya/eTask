package workflow

import (
	"github.com/KKKKjl/eTask/internal/task"
	"github.com/KKKKjl/eTask/internal/utils"
)

// Pipeline represents a pipeline of jobs.
type Pipeliner struct {
	jobs []*task.Message
}

func NewPipeliner(jobs ...*task.Message) Pipeliner {
	for _, job := range jobs {
		if job.ID == "" {
			job.ID = utils.GetUUID()
		}
	}

	// link next job
	for i := len(jobs) - 1; i > 0; i-- {
		jobs[i-1].NextJobId = jobs[i].ID
	}

	return Pipeliner{
		jobs: jobs,
	}
}

func (p *Pipeliner) Jobs() []*task.Message {
	return p.jobs
}

func (p *Pipeliner) Length() int {
	return len(p.jobs)
}

// Group represents a group of jobs.
type Group struct {
	groupUUID string
	jobs      []*task.Message
	jobUUIDS  []string
}

func NewGroup(jobs ...*task.Message) *Group {
	groupUUID := utils.GetUUID()

	jobUUIDS := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if job.ID == "" {
			job.ID = utils.GetUUID()
		}

		jobUUIDS = append(jobUUIDS, job.ID)

		if job.GroupId == "" {
			job.GroupId = groupUUID
		}
	}

	return &Group{
		jobs:      jobs,
		groupUUID: groupUUID,
		jobUUIDS:  jobUUIDS,
	}
}

func (g *Group) GroupUUID() string {
	return g.groupUUID
}

func (g *Group) Jobs() []*task.Message {
	return g.jobs
}

func (g *Group) JobUUIDS() []string {
	return g.jobUUIDS
}

func (g *Group) Length() int {
	return len(g.jobs)
}
