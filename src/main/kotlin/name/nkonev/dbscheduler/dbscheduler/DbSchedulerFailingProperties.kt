package name.nkonev.dbscheduler.dbscheduler

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("db-scheduler-failing")
class DbSchedulerFailingProperties : DbSchedulerAbstractProperties()