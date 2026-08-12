-- Conversation prompts and assistant output are process-local only.
-- This migration removes content retained by older codexD releases.

UPDATE turns
SET input_summary = '[content not retained; ' ||
        length(CAST(COALESCE(queued_input_text, input_summary, '') AS BLOB)) ||
        ' bytes]',
    queued_input_text = NULL,
    error_message_redacted = NULL
WHERE source_kind = 'discord';

UPDATE turns
SET input_summary = '[content not retained; ' ||
        length(CAST(COALESCE(queued_input_text, input_summary, '') AS BLOB)) ||
        ' bytes]',
    queued_input_text = NULL,
    queued_skill_inputs_json = NULL,
    error_message_redacted = NULL
WHERE source_kind = 'schedule';

DELETE FROM message_projections;
DELETE FROM discord_render_plans;

UPDATE tool_projections
SET label = kind,
    summary_json = json_object('content_scrubbed', 1);

UPDATE task_projection_agents
SET safe_message = NULL;

UPDATE task_projections
SET safe_status_summary = operation || ' · ' || state;

UPDATE events
SET payload_json = json_object('content_scrubbed', 1),
    raw_type = NULL;

UPDATE discord_outbox
SET payload_json = json_remove(
        payload_json,
        '$.visible_text',
        '$.plain_text',
        '$.final_answer_text'
    )
WHERE json_extract(payload_json, '$.kind') IN ('turn_final', 'turn_progress');

UPDATE discord_outbox
SET payload_json = json_set(payload_json, '$.agents', json('[]'))
WHERE json_extract(payload_json, '$.kind') = 'task_card';

UPDATE discord_outbox
SET payload_json = json_set(
        json_remove(payload_json, '$.name'),
        '$.name_strategy', 'starter_message',
        '$.name_suffix', COALESCE(
            (
                SELECT substr(i.id, 1, 4)
                FROM ingress_messages i
                WHERE i.thread_creation_outbox_id = discord_outbox.id
            ),
            'task'
        )
    )
WHERE json_extract(payload_json, '$.kind') = 'create_thread';

UPDATE discord_outbox
SET event_sequence = NULL,
    payload_json = json_object(
        'kind', 'retained_tombstone',
        'original_kind', COALESCE(json_extract(payload_json, '$.kind'), 'unknown')
    )
WHERE state = 'superseded';
