Embulk::JavaPlugin.register_filter(
  "base58", "org.embulk.filter.base58.Base58FilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
