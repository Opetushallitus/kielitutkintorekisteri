package fi.oph.kitu.util.scheduling

import com.github.kagkarlsson.scheduler.task.Task
import com.github.kagkarlsson.scheduler.task.helper.Tasks
import fi.oph.kitu.observability.use
import io.opentelemetry.api.trace.Tracer

fun Tracer.recurringTask(
    name: String,
    schedule: String,
    block: () -> Unit,
): Task<Void> =
    Tasks
        .recurring(name, ExtendedSchedules.parse(schedule))
        .execute { _, _ -> traced(name) { block() } }

inline fun <reified T : Any> Tracer.recurringStatefulTask(
    name: String,
    schedule: String,
    initialData: T,
    crossinline block: (T) -> T,
): Task<T> =
    Tasks
        .recurring(name, ExtendedSchedules.parse(schedule), T::class.java)
        .initialData(initialData)
        .executeStateful { taskInstance, _ ->
            traced(name) { block(taskInstance.data ?: initialData) }
        }

@PublishedApi
internal inline fun <R> Tracer.traced(
    taskName: String,
    block: () -> R,
): R =
    spanBuilder(taskName).startSpan().use { span ->
        span.setAttribute("task.name", taskName)
        block()
    }
