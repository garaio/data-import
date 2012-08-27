module DataImport
  class Importer

    def initialize(context, definition, progress_reporter)
      @context = context
      @definition = definition
      @progress_reporter = progress_reporter
    end

    def run
      @definition.writer.transaction do
        @definition.reader.each_row do |row|
          import_row row
          @progress_reporter.inc
        end
        @definition.after_blocks.each do |block|
          @context.instance_exec(@context, &block)
        end
      end
    end

    def map_row(row)
      mapped_row = {}
      @definition.mappings.each do |mapping|
        local_context = @context.build_local_context(:row => row, :mapped_row => mapped_row)
        mapping.apply!(@definition, local_context, row, mapped_row)
      end
      mapped_row
    end

    def import_row(row)
      mapped_row = map_row(row)

      if row_valid?(mapped_row)
        new_id = @definition.writer.write_row(mapped_row)
        @definition.row_imported(new_id, row)

        @definition.after_row_blocks.each do |block|
          local_context = @context.build_local_context(:row => row, :mapped_row => mapped_row)
          local_context.instance_exec(local_context, row, mapped_row, &block)
        end
      end
    end

    def row_valid?(row)
      @definition.row_validation_blocks.all? do |block|
        local_context = @context.build_local_context(:row => row)
        local_context.instance_exec(local_context, row, &block)
      end
    end
    private :row_valid?
  end
end
