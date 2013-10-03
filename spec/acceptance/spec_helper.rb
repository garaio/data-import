require 'rubygems'
require 'bundler/setup'

require 'data-import'
require 'pry'

require File.join(File.dirname(__FILE__), 'support/macros')

RSpec.configure do |config|
  config.extend TestingMacros
end
