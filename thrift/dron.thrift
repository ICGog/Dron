
struct JobInstanceInfo {
  1:string framework,
  2:string jobName
}

service Dron {
  
  void job_instance_registered(1:i32 jobInstanceId,
      2:JobInstanceInfo jobInstanceInfo),

}
