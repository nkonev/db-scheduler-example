package name.nkonev.dbscheduler.dbscheduler

import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.task.ExecutionContext
import com.github.kagkarlsson.scheduler.task.TaskInstance
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask
import com.github.kagkarlsson.scheduler.task.helper.Tasks
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

data class MyTaskData(var id: Long = 0)
data class MyFailingTaskData(var id: Long = 0, var failureCounter: Long = 0)

@SpringBootApplication
class DbSchedulerApplication : CommandLineRunner {

//	@Autowired
//	private lateinit var dbScheduler: Scheduler

	@Autowired
	private lateinit var dataMoverTask: OneTimeTask<MyTaskData>

	override fun run(vararg args: String?) {
		// dbScheduler.schedule(dataMoverTask.instance("1045", MyTaskData(1001L)), Instant.now().plusSeconds(5));
	}
}

//class Wrapper(val value: Any?)

@Configuration
class ClockConfiguration {
	@Bean
	fun clock(): Clock = Clock.systemUTC()
}

const val DATA_MOVER_TASK_NAME = "dataMoverTask"
const val LIMITED_TASK_NAME = "limitedTask"

@Configuration
class DbSchedulerConfig (private val clock: Clock) {

	@Bean(name = [DATA_MOVER_TASK_NAME])
	fun dataMoverTask() : OneTimeTask<MyTaskData> {
		val myAdhocTask: OneTimeTask<MyTaskData> = Tasks.oneTime("data-mover-task", MyTaskData::class.java)
				.execute {   inst: TaskInstance<MyTaskData>, ctx: ExecutionContext? ->
					System.out.println("Executing dataMoverTask! Custom data, Id: " + inst.getData().id + " at " + clock.instant())

				}
		return myAdhocTask
	}

	@Bean(name = [LIMITED_TASK_NAME])
	fun failLimitedTask() : OneTimeTask<MyFailingTaskData> {
		val myAdhocTask: OneTimeTask<MyFailingTaskData> = Tasks.oneTime("fail-limited-task", MyFailingTaskData::class.java)
				.onFailure { executionComplete, executionOperations ->
					if (executionComplete.cause?.get() is RuntimeException) {
						val d: MyFailingTaskData = executionComplete.execution.taskInstance.data as MyFailingTaskData
						val newD = d.copy(failureCounter = d.failureCounter+1)
						if (d.failureCounter < 4) {
							executionOperations.reschedule(executionComplete, Instant.now().plus(2, ChronoUnit.SECONDS), newD)
						} else{
							System.out.println("Giving up on failLimitedTask! Custom data, Id: " + d.id + " at " + clock.instant())
						}
					}
				}
				.execute {   inst: TaskInstance<MyFailingTaskData>, ctx: ExecutionContext? ->
					System.out.println("Executing failLimitedTask! Custom data, Id: " + inst.getData().id + " at " + clock.instant())
					if (inst.getData().id == -42L) {
						throw RuntimeException("Unable to process this number")
					}
				}
		return myAdhocTask
	}

}

@RestController
class MyController {

	@Qualifier(DB_TASK_SCHEDULER)
	@Autowired
	private lateinit var dbTaskScheduler: Scheduler

	@Qualifier(DB_FAILING_TASK_SCHEDULER)
	@Autowired
	private lateinit var dbFailingTaskScheduler: Scheduler

	@Qualifier(DATA_MOVER_TASK_NAME)
	@Autowired
	private lateinit var dataMoverTask: OneTimeTask<MyTaskData>

	@Qualifier(LIMITED_TASK_NAME)
	@Autowired
	private lateinit var limitedTask: OneTimeTask<MyFailingTaskData>

	@PutMapping("/task")
	fun addDataMoverTask(@RequestParam data: Long, @RequestParam afterSeconds: Long) {
		dbTaskScheduler.schedule(dataMoverTask.instance(UUID.randomUUID().toString(), MyTaskData(data)), Instant.now().plusSeconds(afterSeconds));
	}

	@PutMapping("/limited")
	fun addSLimitedTask(@RequestParam data: Long, @RequestParam afterSeconds: Long) {
		dbFailingTaskScheduler.schedule(limitedTask.instance(UUID.randomUUID().toString(), MyFailingTaskData(data)), Instant.now().plusSeconds(afterSeconds));
	}

}


fun main(args: Array<String>) {
	runApplication<DbSchedulerApplication>(*args)
}
