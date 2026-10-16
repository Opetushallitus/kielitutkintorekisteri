package fi.oph.kitu.util.scheduling

import com.github.kagkarlsson.scheduler.task.schedule.Schedule
import com.github.kagkarlsson.scheduler.task.schedule.Schedules

object ExtendedSchedules : Schedules() {
    /**
     * Extended version of `Schedules::parseSchedule`, allowing both regular
     * schedule strings (e.g. `FIXED_DELAY` or `-`) and CRON-based schedules.
     */
    fun parse(value: String?): Schedule =
        try {
            parseSchedule(value)
        } catch (_: UnrecognizableSchedule) {
            cron(value)
        }
}
