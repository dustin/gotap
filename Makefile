include $(GOROOT)/src/Make.inc

.SUFFIXES: .go .$O

OBJS=mc_constants.$O \
		byte_manipulation.$O \
		tap.$O \
		gotap.$O

gotap: $(OBJS)
	$(LD) -o gotap gotap.$O

clean:
	rm -f $(OBJS) gotap

.go.$O:
	$(GC) $<
