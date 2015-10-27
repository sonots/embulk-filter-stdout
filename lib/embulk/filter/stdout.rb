Embulk::JavaPlugin.register_filter(
  "stdout", "org.embulk.filter.stdout.StdoutFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
