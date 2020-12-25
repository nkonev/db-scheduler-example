package name.nkonev.dbscheduler.dbscheduler

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator
import com.github.kagkarlsson.scheduler.CurrentlyExecuting
import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.Serializer
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer
import com.github.kagkarlsson.scheduler.task.ExecutionContext
import com.github.kagkarlsson.scheduler.task.TaskInstance
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask
import com.github.kagkarlsson.scheduler.task.helper.Tasks
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.Instant
import java.util.Optional
import java.util.UUID

data class MyTaskData(var id: Long = 0)

@SpringBootApplication
class DbSchedulerApplication : CommandLineRunner {

	@Autowired
	private lateinit var dbScheduler: Scheduler

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

@Configuration
class DbSchedulerConfig (private val clock: Clock) {

	@Bean
	fun dataMoverTask() : OneTimeTask<MyTaskData> {
		val myAdhocTask: OneTimeTask<MyTaskData> = Tasks.oneTime("data-mover-task", MyTaskData::class.java)
				.execute {   inst: TaskInstance<MyTaskData>, ctx: ExecutionContext? ->
					System.out.println("Executed! Custom data, Id: " + inst.getData().id + " at " + clock.instant())

				}
		return myAdhocTask
	}

	@Bean
	fun dbSchedulerCustomizer() : DbSchedulerCustomizer {
		val typedMapper: ObjectMapper = ObjectMapper()
				.apply {
					Jackson2ObjectMapperBuilder.json().configure(this)
					enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
					enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
					enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE)
					disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
					disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
				}
				.activateDefaultTyping(
						LaissezFaireSubTypeValidator.instance,
						ObjectMapper.DefaultTyping.EVERYTHING,
						JsonTypeInfo.As.WRAPPER_ARRAY
				)

		val customizer = object : DbSchedulerCustomizer {
			override fun serializer(): Optional<Serializer> {
				return Optional.of(object : Serializer {
					override fun serialize(data: Any?): ByteArray {
						return typedMapper.writeValueAsBytes(data)
					}

					override fun <T : Any?> deserialize(clazz: Class<T>?, serializedData: ByteArray?): T {
						return typedMapper.readValue(serializedData, clazz)
					}

				})
			}
		}
		return customizer
	}
}

@RestController
class MyController {

	@Autowired
	private lateinit var dbScheduler: Scheduler

	@Autowired
	private lateinit var dataMoverTask: OneTimeTask<MyTaskData>

	@GetMapping("/task")
	fun getSomething() : List<CurrentlyExecuting> {
		return dbScheduler.currentlyExecuting
	}

	@PutMapping("/task")
	fun addSomething(@RequestParam data: Long, @RequestParam afterSeconds: Long) {
		dbScheduler.schedule(dataMoverTask.instance(UUID.randomUUID().toString(), MyTaskData(data)), Instant.now().plusSeconds(afterSeconds));
	}

}


fun main(args: Array<String>) {
	runApplication<DbSchedulerApplication>(*args)
}
