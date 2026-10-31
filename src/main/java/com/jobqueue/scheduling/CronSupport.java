package com.jobqueue.scheduling;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.springframework.scheduling.support.CronExpression;

/** Cron parsing that accepts both classic 5-field and Spring's 6-field (with seconds) expressions. */
public final class CronSupport {

    private CronSupport() {
    }

    public static CronExpression parse(String cron) {
        if (cron == null || cron.isBlank()) {
            throw new IllegalArgumentException("cron is required");
        }
        String normalized = cron.trim().replaceAll("\\s+", " ");
        if (normalized.split(" ").length == 5) {
            normalized = "0 " + normalized; // fire at second 0 of the matching minute
        }
        try {
            return CronExpression.parse(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid cron '" + cron + "': " + e.getMessage(), e);
        }
    }

    public static ZoneId zone(String zone) {
        try {
            return ZoneId.of(zone == null || zone.isBlank() ? "UTC" : zone);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid time zone '" + zone + "'", e);
        }
    }

    /** Next fire strictly after {@code after}, evaluated in {@code zone} (so DST is handled by the zone rules). */
    public static Instant next(CronExpression cron, ZoneId zone, Instant after) {
        ZonedDateTime next = cron.next(after.atZone(zone));
        if (next == null) {
            throw new IllegalArgumentException("cron expression never fires again");
        }
        return next.toInstant();
    }
}
