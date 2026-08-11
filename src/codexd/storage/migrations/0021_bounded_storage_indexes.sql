CREATE INDEX discord_outbox_dependency_idx
    ON discord_outbox(depends_on_outbox_id)
    WHERE depends_on_outbox_id IS NOT NULL;

CREATE INDEX discord_outbox_coalesce_sequence_idx
    ON discord_outbox(coalesce_key, enqueue_sequence DESC)
    WHERE coalesce_key IS NOT NULL;

CREATE INDEX ingress_messages_thread_creation_outbox_idx
    ON ingress_messages(thread_creation_outbox_id)
    WHERE thread_creation_outbox_id IS NOT NULL;

CREATE INDEX ingress_messages_progress_outbox_idx
    ON ingress_messages(progress_outbox_id)
    WHERE progress_outbox_id IS NOT NULL;

CREATE INDEX schedule_drafts_confirmation_outbox_idx
    ON schedule_drafts(confirmation_outbox_id)
    WHERE confirmation_outbox_id IS NOT NULL;

CREATE INDEX dynamic_tool_invocations_outbox_idx
    ON dynamic_tool_invocations(outbox_id)
    WHERE outbox_id IS NOT NULL;

CREATE INDEX discord_outbox_event_sequence_idx
    ON discord_outbox(event_sequence)
    WHERE event_sequence IS NOT NULL;

CREATE INDEX message_projections_event_sequence_idx
    ON message_projections(last_event_sequence);

CREATE INDEX tool_projections_event_sequence_idx
    ON tool_projections(last_event_sequence);

CREATE INDEX task_projections_event_sequence_idx
    ON task_projections(last_event_sequence);
