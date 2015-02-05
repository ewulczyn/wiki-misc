import re


#dbname: language_code+site_code


production_sites = [
'wikipedia',
'wiktionary',
'wikibooks',
'wikinews',
'wikiquote',
'wikisource',
'wikiversity',
'wikivoyage',]


regex = '(.*)\\.(m\\.|zero\\.|wap\\.)?('+'|'.join(production_sites)+')\\.org'


b = re.compile(regex)

for line in open('hosts.txt'):
