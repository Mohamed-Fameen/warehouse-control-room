package com.warehouse.common;

import com.warehouse.common.layout.Cell;

/**
 * Command from a dispatcher to one robot on {@link Topics#robotCommand} (QoS 1, not retained).
 * The robot answers on {@link Topics#robotJob} with a {@link RobotJobUpdate}. MQTT QoS 1 may
 * deliver a command twice; a robot that already holds {@code jobId} just repeats its state.
 *
 * @param pick aisle cell to stop at; the shelf is directly east or west of it
 * @param ts   epoch ms when the command was sent
 */
public record RobotCommand(RobotCommandType type, String jobId, Cell pick, long ts) {
}
