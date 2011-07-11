include $(GOROOT)/src/Make.inc

.SUFFIXES: .go .$O

OBJS=mc_constants.$O \
		byte_manipulation.$O \
		tap.$O \
		tap_example.$O

tap_example: $(OBJS)
	$(LD) -o $@ $@.$O

clean:
	rm -f $(OBJS) tap_example

.go.$O:
	$(GC) $<
