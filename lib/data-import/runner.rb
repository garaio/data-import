module DataImport
  class Runner

    def initialize(plan, progress_reporter = ProgressBar)
      @plan = plan
      @progress_reporter = progress_reporter
    end

    def run(options = {})
      dependency_resolver = DependencyResolver.new(@plan)
      resolved_plan = dependency_resolver.resolve(:run_only => options[:only])
      resolved_plan.definitions.each do |definition|
        bar = @progress_reporter.create(:title => definition.name, :total => definition.total_steps_required, format: '%t %p%% %B %e', :output => progress_stream)

        DataImport.logger.info "Starting to import \"#{definition.name}\""
        context = ExecutionContext.new(resolved_plan, definition, bar)
        definition.run context

        bar.finish
      end
    end

    def progress_stream
      @progress_stream ||= begin
        IO.new(3, 'w')
      rescue Errno::EBADF, ArgumentError
        File.open('/dev/tty', 'w')
      end
    end

  end
end
