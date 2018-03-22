# Data Sanity Check

### Absence of data might be due to

External factors:

- the journal stopped to be published for some years/months/days
- publication periodicity

Preservation factors:

- paper copies were lost and never digitized
- digital copies were lost


### To check

***Text/Images***  
 
- for each text page, there should be a corresponding image
- for each article, there should be text

***Olive***   

- check the presence/absence of issue pdfs
- check the presence/absence of page pdfs
- check presence of .zip per issue
- if not tifs, check presence of png

### Mapping the coverage

In the DB, store information so as to be able to (among others):

- detect missing/absent years of a journal
- detect missing/absent months of a year
- detect missing/absent days of a month

## Discussion 22.03

**Data to cross-check:**   
- canonical json (S3)
- images (NAS)

**Sanity check strategies:**   

1) isolated checks for 1) text and 2) images, w.r.t. original data. To be run apart.

2) cross-check on generated canonical data, assumin that 1) was fine.

**Isolated check images**
- corrupted archives
- image format list per issue
- per journal, total sum jp2 size for NP
- per journal, total sum of all images

Goes in the DB:
- in the journal table, how many images comes tif/png/jpg + %


**Isolated check text**

- check pages without OCR (empty regions)
- corrupted archives
- per journal, total size of json files
- checking the xml for the articles

Goes in the DB:
- numb. of corrupted archives(issues) per journal
- numb. of pages without OCR per journal

**Cross-check on file system:**

- Granularity level: issue (canonical id)
- What needs to be checked:   
  - number of json page = number of image jp2
 
- output: csv with everything

**Sanity check on the DB**



