package mcp

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// confirmationProtocol is appended to every prompt that can reach a write
// tool. Tool names in prompt text must exist in Catalog(); a test enforces it.
const confirmationProtocol = `
Write tools are two-phase:
1. Call the tool WITHOUT "confirmation". Nothing changes; you get a preview, a confirmation token and a restConfirmation note.
2. Show the preview and token to the human and STOP. Ask them to approve before you continue.
3. To commit, repeat the identical call (same arguments) with "confirmation" set to the token. Never change arguments between the two calls.
Values the REST request body itself requires (body.confirmation, planHash) are separate from the MCP token; the prepare result's restConfirmation field says which. Ask the human before committing any destructive step.`

type promptDef struct {
	prompt *sdk.Prompt
	text   func(args map[string]string) string
}

func arg(name, desc string) *sdk.PromptArgument {
	return &sdk.PromptArgument{Name: name, Description: desc, Required: true}
}

func val(args map[string]string, name string) string {
	if v := strings.TrimSpace(args[name]); v != "" {
		return v
	}
	return "<" + name + ">"
}

var promptDefs = []promptDef{
	{
		prompt: &sdk.Prompt{Name: "diagnose-consumer-lag", Title: "Diagnose consumer lag", Description: "Investigate why a consumer group is lagging. Read-only.",
			Arguments: []*sdk.PromptArgument{arg("cluster", "Cluster id"), arg("group", "Consumer group id")}},
		text: func(a map[string]string) string {
			return fmt.Sprintf(`Diagnose consumer lag for group %q on cluster %q. This is read-only; do not change anything.
Call these tools in order:
1. kaflux_group_list (cluster=%q, search=%q) to confirm the group exists and see its state.
2. kaflux_group_get (cluster=%q, group=%q) for members, assignments and per-partition lag.
3. kaflux_topic_get (cluster=%q, topic=<each lagging topic>) for partition count, leaders and in-sync replicas.
4. kaflux_broker_list (cluster=%q) to check that the brokers hosting lagging partitions are healthy.
Report which partitions lag most, whether the group is rebalancing or has idle members, and the likely cause.`,
				val(a, "group"), val(a, "cluster"), val(a, "cluster"), val(a, "group"), val(a, "cluster"), val(a, "group"), val(a, "cluster"), val(a, "cluster"))
		},
	},
	{
		prompt: &sdk.Prompt{Name: "topic-health-review", Title: "Review topic health", Description: "Review a topic's configuration, balance and consumers. Read-only.",
			Arguments: []*sdk.PromptArgument{arg("cluster", "Cluster id"), arg("topic", "Topic name")}},
		text: func(a map[string]string) string {
			return fmt.Sprintf(`Review the health of topic %q on cluster %q. This is read-only; do not change anything.
Call these tools in order:
1. kaflux_topic_get (cluster=%q, topic=%q) for partitions, replication and under-replicated state.
2. kaflux_topic_config_get (cluster=%q, topic=%q) for retention, cleanup policy and non-default settings.
3. kaflux_topic_balance (cluster=%q, topic=%q) for how evenly replicas and leaders are spread.
4. kaflux_group_list (cluster=%q) to find consumer groups reading this topic.
Summarize risks (under-replication, skew, retention mistakes, unused topic) and propose next steps for a human to approve.`,
				val(a, "topic"), val(a, "cluster"), val(a, "cluster"), val(a, "topic"), val(a, "cluster"), val(a, "topic"), val(a, "cluster"), val(a, "topic"), val(a, "cluster"))
		},
	},
	{
		prompt: &sdk.Prompt{Name: "plan-safe-rebalance", Title: "Plan a safe rebalance", Description: "Plan, validate and (with human approval) execute a partition rebalance.",
			Arguments: []*sdk.PromptArgument{arg("cluster", "Cluster id")}},
		text: func(a map[string]string) string {
			c := val(a, "cluster")
			return fmt.Sprintf(`Plan a safe partition rebalance on cluster %q.
Call these tools in order:
1. kaflux_balance_get (cluster=%q) to see broker skew and whether a rebalance is warranted.
2. kaflux_broker_list (cluster=%q) to confirm every broker is healthy.
3. kaflux_rebalance_list (cluster=%q) to make sure no rebalance is already running.
4. kaflux_rebalance_plan (cluster=%q, body={topics, brokers, rackAware, throttleBytesPerSec}) to generate a plan; this only records a plan.
5. kaflux_rebalance_get (cluster=%q, job=<job id>) to read the plan, its planHash and moves.
6. kaflux_rebalance_dry_run (cluster=%q, job=<job id>, body={confirmation:true, planHash}) to validate it against the live cluster.
7. Show the plan and dry-run warnings to the human and wait for approval.
8. Only after approval: kaflux_rebalance_execute (cluster=%q, job=<job id>, body={confirmation:true, planHash}).
9. Monitor with kaflux_rebalance_get and kaflux_rebalance_events; use kaflux_rebalance_pause or kaflux_rebalance_cancel only if the human asks.
%s`, c, c, c, c, c, c, c, c, confirmationProtocol)
		},
	},
	{
		prompt: &sdk.Prompt{Name: "audit-acl-changes", Title: "Audit ACL changes", Description: "Review ACLs and recent changes on a cluster. Read-only.",
			Arguments: []*sdk.PromptArgument{arg("cluster", "Cluster id")}},
		text: func(a map[string]string) string {
			c := val(a, "cluster")
			return fmt.Sprintf(`Audit ACLs and recent access changes on cluster %q. This is read-only; do not change anything.
Call these tools in order:
1. kaflux_acl_list (cluster=%q) for the current ACL bindings.
2. kaflux_audit_cluster (cluster=%q) for recent audit events; focus on ACL create and delete actions and note actor, time and outcome.
Flag wildcard principals or resources, overly broad operations and recent deletions. Report findings only. If the human wants a change, tell them it needs kaflux_acl_create or kaflux_acl_delete, which use the prepare/commit protocol: call without "confirmation" to get a preview and token, show it to the human and stop until they approve; commit by repeating the same arguments with "confirmation". Ask before any destructive step.`, c, c, c)
		},
	},
}

// addPrompts registers the prompts. They are static text, so every token
// sees them; the tools they name still enforce the token's visibility.
func addPrompts(s *sdk.Server) {
	for _, def := range promptDefs {
		s.AddPrompt(def.prompt, func(_ context.Context, req *sdk.GetPromptRequest) (*sdk.GetPromptResult, error) {
			return &sdk.GetPromptResult{
				Description: def.prompt.Description,
				Messages:    []*sdk.PromptMessage{{Role: "user", Content: &sdk.TextContent{Text: def.text(req.Params.Arguments)}}},
			}, nil
		})
	}
}
