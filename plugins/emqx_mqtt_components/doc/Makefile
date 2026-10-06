.DEFAULT_GOAL := diagrams
.NOTPARALLEL:

MERMAID_VERSION := 11.12.0
MMDC ?= npx --yes --package @mermaid-js/mermaid-cli@$(MERMAID_VERSION) mmdc
FORMAT_CODE = npx --yes --package @mermaid-js/mermaid-cli@$(MERMAID_VERSION) -c 'node format-code.mjs "$$(command -v mmdc)" $(1) $(SCALE) $(PUPPETEER_CONFIG) $(SOURCE)'
SOURCE ?= client_flows
SCALE ?= 3
BACKGROUND ?= white
PUPPETEER_CONFIG ?= puppeteer.json

FLAGS = --configFile mermaid.json --cssFile slides.css \
	--puppeteerConfigFile $(PUPPETEER_CONFIG) --backgroundColor $(BACKGROUND) --width 1920

.PHONY: diagrams svg png clean

diagrams: png

svg: | images
	$(MMDC) $(FLAGS) --input $(SOURCE).md --output images/$(SOURCE).svg
	$(call FORMAT_CODE,svg)

png: svg
	$(call FORMAT_CODE,png)

images:
	mkdir -p $@

clean:
	rm -rf images
