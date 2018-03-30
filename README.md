# Data Sanity Check

### Absence of data might be due to

External factors:

1. the journal stopped to be published for some years/months/days
2. publication periodicity

Preservation factors:

3. paper copies were lost and never digitized
4. digitization happened but digital copies were lost or damaged.

Transparency about cases 1 and 2 supposes external knowledge, and should be encoded as metadata.
Case 3 is the same, but it is not sure that this info is always nicely kept.
Case 4 can detected via quality control of digitization process, or simply via their usage.



### To check

***Text/Images***  
 
- for each text page, there should be a corresponding image
- for each article, there should be text

***Olive***   

- check the presence/absence of issue pdfs
- check the presence/absence of page pdfs
- check presence of .zip per issue
- if not tifs, check presence of png and/or jpg

### Mapping the coverage

In the DB, store information so as to be able to (among others):

- detect missing/absent years of a journal
- detect missing/absent months of a year
- detect missing/absent days of a month
- detect missing image for a page
- detect missing article? => Is this possible?


## Discussion 22.03.18

**Data to cross-check:**   
- canonical json (S3)
- images (NAS)

**Sanity check strategy:**   

1. isolated checks for 1) text and 2) images, w.r.t. original data. To be run apart.

2. cross-check on generated canonical data, assuming that 1) was fine.

**Isolated check images**
- detect corrupted archives
- overview of image format per journal, counts based on issues.
- per journal, total sum of jp2 file size
- per journal, total sum of all images (?)

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



