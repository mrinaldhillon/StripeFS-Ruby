#!/usr/bin/env ruby

require 'pathname'

module Paths
		
	def self.getstriped_paths(stripes_a, path)
			striped_paths = []
			pathname = ::Pathname.new(path)
			return striped_paths << @root.dup if pathname.root?
			
			striped_paths = stripes_a.dup
			pathname.each_filename do |filename|
				striped_paths.each_with_index do |stripe, index| 
					striped_paths[index] = striped_paths[index] + "#{filename}.#{index+1}"
				end
			end
			striped_paths
	end

	stripes = ["/tmp/1", "/tmp/2", "/tmp/3"]
	stripes_a = []
	Array(stripes).each_with_index do |stripe, index|
		stripes_a[index] = ::Pathname.new(stripe)
#		raise ArgumentError unless stripes_a[index].directory?
	end

	paths = getstriped_paths(stripes_a, "/1/2/3/4/5/6/7/8/9/10")
	puts paths.inspect
end
