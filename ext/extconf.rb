#puts "running make"
#`make build`
require 'mkmf'
find_executable('go')
$objs = []
def $objs.empty?; false ;end
create_makefile("ruby_snowflake_client_ext")
case `#{CONFIG['CC']} --version`
when /Free Software Foundation/
  ldflags = '-Wl,--unresolved-symbols=ignore-all'
when /clang/
  ldflags = '-undefined dynamic_lookup'
end
File.open('Makefile', 'a') do |f|
  f.write <<eom.gsub(/^ {8}/, "\t")
$(DLLIB): Makefile $(srcdir)/ruby_snowflake.go
        CGO_CFLAGS='$(INCFLAGS)' CGO_LDFLAGS='#{ldflags}' \
  go build -p 4 -buildmode=c-shared -o $(DLLIB) .
eom
end

