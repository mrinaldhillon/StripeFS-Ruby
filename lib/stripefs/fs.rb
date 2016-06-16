require 'rfuse'
require 'pathname'
require_relative 'utils'
require "stringio"
require "fileutils"
module StripeFS
	class FS
		attr_reader :chunksize, :stripes, :root, :rootstat
			
		def initialize(mountpath, stripes, chunksize)
			raise ArgumentError if Utils.is_blank?(stripes) || Utils.is_blank?(mountpath)
			
			@rootstat = ::Pathname.new(mountpath).lstat
			@root = ::Pathname.new(mountpath)
			raise ArgumentError unless @root.directory?
			
			@chunksize = chunksize
			@uid = ::Process.uid
			@gid = ::Process.gid

			@stripes = []
			Array(stripes).each_with_index do |stripe, index|
				@stripes[index] = ::Pathname.new(stripe)
				raise ArgumentError unless @stripes[index].directory?
			end
		end

		INIT_TIMES = Array.new(3,0)
		
		def lstat(pathname)
			return @rootstat if pathname.to_s == @root.to_s
			pathname.lstat
		end

		def method_missing(method, *args)
			puts method
			puts "method_missing"
		end

		def statfs(ctx,path)
			s = ::RFuse::StatVfs.new()
			s.f_bsize    = 1024
			s.f_frsize   = 1024
			s.f_blocks   = 1000000
			s.f_bfree    = 500000
			s.f_bavail   = 990000
			s.f_files    = 10000
			s.f_ffree    = 9900
			s.f_favail   = 9900
			s.f_fsid     = 23423
			s.f_flag     = 0
			s.f_namemax  = 10000
			return s
		end

		def typeofstat(type)
			case type
			when "directory"
			return ::RFuse::Stat::S_IFDIR
			when "file"
			return ::RFuse::Stat::S_IFREG
			when "link"
			return ::RFuse::Stat::S_IFLNK
			when "characterSpecial"
			return ::RFuse::Stat::IFCHR
			when "blockSpecial"
			return ::RFuse::Stat::IFBLK
			when "fifo"
			return ::RFuse::Stat::IFIFO
			when "socket"
			return ::RFuse::Stat::IFSOCK
			else
			return ::RFuse::Stat::S_IFMT
			end
		end
		
		# stripe path i.e. /1/2/3 => /1.1/2.1/3.1
		def getstriped_paths(path)
			striped_paths = @stripes.dup
			return striped_paths if path == "/"
			
			pathname = ::Pathname.new(path)
			pathname.each_filename do |filename|
				striped_paths.each_with_index do |stripe, index| 
					striped_paths[index] = striped_paths[index] + "#{filename}.#{index+1}"
				end
			end
			striped_paths
		end

		def getattr(ctx, path)
			if path == "/"
				stat = @rootstat.dup
			else
				striped_paths = getstriped_paths(path)
				stat = striped_paths[0].lstat
			end

			values = {:uid => @uid, :gid => @gid, :size => stat.size,
				:atime => stat.atime, :mtime => stat.mtime, :ctime => stat.ctime, :dev => stat.dev, :ino => stat.ino, 
				:nlink => stat.nlink, :rdev => stat.rdev, :blksize => stat.blksize, :blocks => stat.blocks}
			type = typeofstat(stat.ftype)
			return ::RFuse::Stat.new(type, stat.mode, values) if type == ::RFuse::Stat::S_IFDIR ||
																														type == ::RFuse::Stat::S_IFLNK

			striped_paths.each_with_index do |stripe, index|
				next if index == 0
				values[:size] += stripe.size
			end
			return ::RFuse::Stat.new(type, stat.mode, values)
		end

		def fgetattr(ctx, path, ffi)
			getattr(ctx, path)
		end

		def readdir(ctx, path, filler, offset, ffi)
			striped_paths = getstriped_paths(path)
			
			 filler.push(".",nil,0)
			 filler.push("..",nil,0)

			 striped_paths[0].each_child(false) do |child|
			 	filler.push(child.to_s.chomp(".1"), nil, 0)
			end
		end	 	

		def mkdir(ctx, path, mode)
			striped_paths = getstriped_paths(path)
			striped_paths.each { |stripe| stripe.mkdir(mode) }
		end

		def rmdir(ctx, path)
			striped_paths = getstriped_paths(path)
			striped_paths.each { |stripe| stripe.rmdir }
		end

		def chmod(ctx, path, mode)
			getstriped_paths(path).each { |stripe| stripe.chmod(mode) }
		end
		
		def chown(ctx, path, uid, gid)
			getstriped_paths(path).each { |stripe| stripe.chown(uid, gid) }
		end
		
		def rename(ctx, from, to)
			striped_topaths = getstriped_paths(to)
			getstriped_paths(from).each_with_index { |stripe, index| stripe.rename(striped_topaths[index].to_s) }
		end
		
		def open(ctx, path, ffi)
			puts "In Open, FFI: #{ffi.flags}, path: #{path}"
			getstriped_paths(path).each do |stripe| 
				stripe.open(ffi.flags) { |f| } #open and close the file by calling open with block
			end
		end

		def create(ctx, path, mode, ffi)
			puts "In Create, FFI: #{ffi.flags}, path: #{path}"
			getstriped_paths(path).each do |stripe| 
				puts stripe
				stripe.open("a+") do |f| 
					f.chmod(mode)
					puts f.path
				end #open and close the file by calling open with block
			end
		end

=begin

Also
		
		1.Find which stripe to begin writing
	Begin
		2. Find Offset in Split to write at.
		3. Calculate size to write based on chunk boundary
			4. If chunk boundary overflows than continue to next stripe and repeat at step 2..4
	end
1.
	chunk_num_at_offset = offset/chunksize # find number of chunks till offset  
	write_at_stripe_num = chunknum_at_offset % stripe_count


1. Find out which stripe to begin writing
		ChunknumAtOffset = offset/chunksize #find number chunks till offset in the file
2. SplitnumAtOffset= ChunknumAtOffset%SplitCount #find which stripe chunk and offset with lie
2. Write can be across boundaries of stripes
3. For each stripe with starting stripe
1. Calculate offset in that stripe to start writing at
1. OffsetInChunk (offset % chunk size) #find location of offset relative a single chunk’s boundary
2. ChunknumInSplit = ChunknumAtOffset/SplitCount # gives no. chunks till offset in the stripe
3. OffsetInSplit = (ChunknumInSplit*Chunksize) + OffsetInChunk
2. Calculate Size to write at chunk in stripe
1. SizeAvailableToWrite = Chunksize - OffsetInChunk
2. if SizeToWrite > SizeAvailableToWrite
1. then SizeInChunkAtSplit = SizeAvailableToWrite and SizeToWrite -= SizeInChunkAtSplit
2. else SizeInChunkAtSplit = SizeToWrite
4. Write at OffsetInSplit a buffer of SizeinChunkAtSplit

=end

		def stripe_write(striped_paths, io, offset, chunksize, ffi)
			stripes_count = striped_paths.count
			# Find which stripe to begin writing
			num_chunks_till_offset_in_file = offset/chunksize
			num_stripe_at_offset = num_chunks_till_offset_in_file % stripes_count

			size = io.length - io.tell

			# Find offset in stripe to write at
			offset_in_chunk_in_stripe = offset % chunksize	#offset relative to a chunk 
			num_chunk_till_offset_in_stripe = num_chunks_till_offset_in_file / stripes_count
			offset_in_stripe = (num_chunk_till_offset_in_stripe * chunksize) + offset_in_chunk_in_stripe

			# Calculate size to write at offset_in_stripe
			size_available_in_chunk = chunksize - offset_in_chunk_in_stripe
			size_to_write_in_stripe = size > size_available_in_chunk ? size_available_in_chunk : size
			
			# write data of calcalated size at offset in stripe
			::File.open(striped_paths[num_stripe_at_offset], "r+") do |file|
				puts "In Stripe size #{size}, offset_in_stripe #{offset_in_stripe}, size of write in stripe #{size_to_write_in_stripe}"
				file.seek(offset_in_stripe, 0)
				offset += file.write(io.read(size_to_write_in_stripe))
			end
				puts "New offset in file #{offset}"
			offset
		end


		def write(ctx, path, data, offset, ffi)
			puts "Write Called at #{path} at offset #{offset}"
			chunksize = @chunksize
			striped_paths = getstriped_paths(path)
			offset_in_file = offset
			io = StringIO.new(data)
			size_of_buffer = io.length
			puts "Size of Data #{size_of_buffer}"
			puts "Expected final offset = Offset + Length of buffer = #{offset + size_of_buffer}"
			return 0 if 0 == io.length
			begin
				offset_in_file = stripe_write(striped_paths, io, offset_in_file, chunksize, ffi)
			end while io.tell != size_of_buffer
			
			puts "Final offset #{offset_in_file}"
			length_written = offset_in_file - offset
			puts "Length written #{length_written}"
			length_written
		end

		def stripe_read(striped_paths, size, offset, chunksize, read_length, ffi)
			stripes_count = striped_paths.count
			# Find which stripe to begin writing
			num_chunks_till_offset_in_file = offset/chunksize
			num_stripe_at_offset = num_chunks_till_offset_in_file % stripes_count

			size -= read_length
			read_buffer = ""
			# Find offset in stripe to write at
			offset_in_chunk_in_stripe = offset % chunksize	#offset relative to a chunk 
			num_chunk_till_offset_in_stripe = num_chunks_till_offset_in_file / stripes_count
			offset_in_stripe = (num_chunk_till_offset_in_stripe * chunksize) + offset_in_chunk_in_stripe

			# Calculate size to write at offset_in_stripe
			size_to_read_in_chunk = chunksize - offset_in_chunk_in_stripe
			size_to_read_in_stripe = size > size_to_read_in_chunk ? size_to_read_in_chunk : size
			
			# write data of calcalated size at offset in stripe
			::File.open(striped_paths[num_stripe_at_offset], "r+") do |file|
				puts "In Stripe #{file.path} of size #{size}, offset_in_stripe #{offset_in_stripe}, size of read in stripe #{size_to_read_in_stripe}"
				file.seek(offset_in_stripe, 0)
				read_buffer = file.read(size_to_read_in_stripe)
				puts "nil read at #{offset_in_stripe} with read of #{size_to_read_in_stripe}" if read_buffer.nil? 
			end
			read_buffer
		end

		def read(ctx, path, size, offset, ffi)
			puts "Read called at #{path} at offset #{offset}, size #{size}"
			chunksize = @chunksize
			striped_paths = getstriped_paths(path)
			read_length = 0
			buffer = ""
			read_buffer = ""

			begin 
				read_buffer = stripe_read(striped_paths, size, offset, chunksize, read_length, ffi)
				if !read_buffer.nil?
					buffer << read_buffer 				
					len = read_buffer.length
					read_length += len
			 		offset += len
				end	
			end while (!read_buffer.nil? && read_length < size)
			puts "read length = #{read_length}"
			buffer
		end

		def release(ctx, path, ffi)
			puts "release #{path} with ffi #{ffi.flags}"
		end
		
		def unlink(ctx, path)
			getstriped_paths(path).each { |path| path.unlink }
		end

		def utimens(ctx, path, atime, mtime)
			puts "Utime #{atime} #{atime.class} #{mtime}"
			getstriped_paths(path).each { |path| path.utime(atime/1000000000, mtime/1000000000) }
		end
		
=begin Truncate the file to size offset i.e. truncate it till offset not including
 Use logic in write and read to find stripe in offset to begin.
 After find the stripe at offset, find the same for rest of the stripes just once.
 Truncate each at the calculated offset
=end

		def truncate(ctx, path, length)
			striped_paths = getstriped_paths(path)
			stripes_count = striped_paths.count
			chunksize = @chunksize
			remaining_length = 0
			bytes_to_skip = 0
			stripe_final_size = []

# Initialize truncate size for each stripe path to multiple of chunk size for complete iteration
			(0..stripes_count-1).each_with_index do |index|
				break if length > (chunksize * stripes_count)
				stripe_final_size[index] = (length / (chunksize * stripes_count)) * chunksize
			end
# Calculate final truncate size for last incomplete iteration through stripes
		remaining_length = length % (chunksize * stripes_count)

			(0..stripes_count-1).each_with_index do |index|
				break if remaining_length < 0
				bytes_to_skip = (remaining_length - chunksize) > 0 ? chunksize : remaining_length
				remaining_length -= bytes_to_skip;
				stripe_final_size[index] += bytes_to_skip;
			end

# /stripefs/file --> /stripe1/file.1, /stripe2/file.2, /stripe3/file.3 ....
			striped_paths.each_with_index do |stripe, index|
				stripe.truncate(stripe_final_size[index])
			end
		end

		def ftruncate(ctx, path, length, ffi)
			truncate(ctx, path, length)
		end
=begin		
		def link(ctx, from, to)
			puts "Link #{from}, #{to}"
			from_striped_paths = getstriped_paths(from)
			to_striped_paths = getstriped_paths(to)

			from_striped_paths.each_with_index do |stripe, index| 
				::FileUtils.ln(stripe.to_s, to_striped_paths[index].to_s)
			end
		end

		def symlink(ctx, from, to)
			puts "Symlink #{from}, #{to}"
			from_striped_paths = getstriped_paths(from)
			to_striped_paths = getstriped_paths(to)

			from_striped_paths.each_with_index do |stripe, index| 
				::FileUtils.ln_s(stripe.to_s, to_striped_paths[index].to_s)
			end
		end

		def readlink(ctx, path, size)
			striped_paths = getstriped_paths(path)
			::File.readlink(striped_paths[0].to_s)
		end
=end
	end
end
