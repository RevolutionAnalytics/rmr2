TESTS := $(wildcard pkg/tests/*.R)
OUTPUT := $(addprefix out/,$(notdir $(TESTS:.R=.out)))

check: $(OUTPUT)

out/%.out:pkg/tests/%.R
	R CMD BATCH  --vanilla --slave $<  $@

clean:
	rm -f $(OUTPUT)
	rm -rf rmr-* job_local*
