package name.nkonev.dbscheduler.dbscheduler

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator
import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.SchedulerName
import com.github.kagkarlsson.scheduler.Serializer
import com.github.kagkarlsson.scheduler.task.Task
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy
import javax.sql.DataSource

const val DB_TASK_SCHEDULER = "dbTaskScheduler"
const val DB_FAILING_TASK_SCHEDULER = "dbFailingTaskScheduler"

@Configuration
@EnableConfigurationProperties(DbSchedulerFailingProperties::class, DbSchedulerTaskProperties::class)
@ConditionalOnProperty(value = ["db-scheduler.enabled"], matchIfMissing = true)
class DbSchedulerConfiguration(private val existingDataSource: DataSource) {

    /**
     * Provide an empty customizer if not present in the context.
     */
    @Bean
    fun serializer() : Serializer {
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

        val customizer = object : Serializer {
            override fun serialize(data: Any?): ByteArray {
                return typedMapper.writeValueAsBytes(data)
            }

            override fun <T : Any?> deserialize(clazz: Class<T>?, serializedData: ByteArray?): T {
                return typedMapper.readValue(serializedData, clazz)
            }

        }
        return customizer
    }


    private fun makeScheduler(config: DbSchedulerAbstractProperties, serializer: Serializer, configuredTasks: List<Task<*>>): Scheduler {
        log.info("Creating db-scheduler using tasks from Spring context: {}", configuredTasks)

        // Ensure that we are using a transactional aware data source
        val transactionalDataSource = configureDataSource(existingDataSource)

        // Instantiate a new builder
        val builder = Scheduler.create(transactionalDataSource, configuredTasks)
        builder.threads(config.threads)

        // Polling
        builder.pollingInterval(config.pollingInterval)
        config.pollingLimit.ifPresent { pollingLimit: Int? -> builder.pollingLimit(pollingLimit!!) }
        builder.heartbeatInterval(config.heartbeatInterval)

        // Use scheduler name implementation from customizer if available, otherwise use
        // configured scheduler name (String). If both is absent, use the library default
//        if (customizer.schedulerName().isPresent) {
//            builder.schedulerName(customizer.schedulerName().get())
//        } else if (config.schedulerName != null) {
//            builder.schedulerName(SchedulerName.Fixed(config.schedulerName))
//        }
        builder.tableName(config.tableName)

        // Use custom serializer if provided
        builder.serializer(serializer)
        if (config.isImmediateExecutionEnabled) {
            builder.enableImmediateExecution()
        }

        // Use custom executor service if provided
//        customizer.executorService().ifPresent { executorService: ExecutorService? -> builder.executorService(executorService) }
        builder.deleteUnresolvedAfter(config.deleteUnresolvedAfter)

        return builder.build()
    }

    @Bean(destroyMethod = "stop", initMethod = "start", name = [DB_TASK_SCHEDULER])
    fun dataMoverScheduler(config: DbSchedulerTaskProperties, customizer: Serializer, tasks: List<OneTimeTask<MyTaskData>>): Scheduler {
        return makeScheduler(config, customizer, tasks)
    }

    @Bean(destroyMethod = "stop", initMethod = "start", name = [DB_FAILING_TASK_SCHEDULER])
    fun failingScheduler(config: DbSchedulerFailingProperties, customizer: Serializer, tasks: List<OneTimeTask<MyFailingTaskData>>): Scheduler {
        return makeScheduler(config, customizer, tasks)
    }

    companion object {
        private val log = LoggerFactory.getLogger(DbSchedulerConfiguration::class.java)
        private fun configureDataSource(existingDataSource: DataSource): DataSource {
            if (existingDataSource is TransactionAwareDataSourceProxy) {
                log.debug("Using an already transaction aware DataSource")
                return existingDataSource
            }
            log.debug("The configured DataSource is not transaction aware: '{}'. Wrapping in TransactionAwareDataSourceProxy.", existingDataSource)
            return TransactionAwareDataSourceProxy(existingDataSource)
        }
    }

}