module DataImport
  module Sequel
    class Dataset

      BATCH_SIZE = 1000

      def initialize(connection, base_query_block, options={})
        @connection = connection
        @base_query_block = base_query_block
        @order_by = options[:order_by].is_a?(Array) ? options[:order_by] : options[:order_by] ? [options[:order_by]] : []
      end

      def each_row(&block)
        iterate_dataset(selection, &block)
      end

      def selection
        query = base_query
        # query.order_by() doesn't work with Datasets created from SQL
        query.opts[:sql] = query.opts[:sql]+"\nORDER BY #{@order_by.join(', ')}\n" if @order_by.size > 0
        query
      end

      def count
        begin
          base_query.count
        rescue Exception => exception
          DataImport.logger.error "*** Error for statement #{base_query.sql} ***"
          raise
        end
      end

      def base_query
        @base_query_block.call(@connection)
      end

      def iterate_dataset(dataset, &block)
        dataset.each do |row|
          @connection.before_filter.call(row) if @connection.before_filter
          block.call(row)
        end
      end
      private :iterate_dataset
    end
  end
end
