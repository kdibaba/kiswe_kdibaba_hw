from django.core.management.base import BaseCommand, CommandError
from company.models import (Company,
                            CompanyLocation,
                            Technology,
                            Industry,
                            ExternalJobListing,
                            Benefit,
                            BenefitTitle,
                            BenefitCategory,
                            )
from company.utils import formatted_company_name
from scraper.models import DiceCompany
from optparse import make_option
from django.utils import timezone
from unicodedata import normalize
from django.db.models import Count
from django.contrib.gis import geos

from dateutil.parser import parse as date_parser

import os, shutil, re, feedparser, requests
import urllib
import json
from datetime import datetime, timedelta

US_STATES = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
US_STATES_FULL = ['Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentuck','Louisiana','Maine','Maryland','Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey','New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennesse','Texas','Utah','Vermont','Virginia','Washington','West Virginia','Wisconsin','Wyoming']
JOBS_EXPIRE_DAYS = 30

class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
         make_option('--verbose',
             dest='verbose',
             action='store_true',
             help='Verbose mode. Display all actions on screen.'),
         make_option('--dry-run',
             dest='dry_run',
             action='store_true',
             help='Dry run.. nothing is executed.'),
         make_option('--get-sof-jobs',
             dest='get_sof_jobs',
             action='store_true',
             help='Get stackoverflow Jobs from external apis'),
         make_option('--get-dice-jobs',
             dest='get_dice_jobs',
             action='store_true',
             help='Get dice Jobs from external apis'),
         make_option('--get-indeed-jobs',
             dest='get_indeed_jobs',
             action='store_true',
             help='Get indeed Jobs from external apis'),
         make_option('--get-zip-jobs',
             dest='get_zip_jobs',
             action='store_true',
             help='Get ziprecruiter Jobs from external apis'),
         make_option('--get-all-jobs',
             dest='get_all_jobs',
             action='store_true',
             help='Get all Jobs from external apis'),
         )
    def handle(self, *args, **options):
        self.verbose = options.get('verbose', False)
        self.dry_run = options.get('dry_run', False)
        self.get_sof_jobs = options.get('get_sof_jobs', False)
        self.get_dice_jobs = options.get('get_dice_jobs', False)
        self.get_indeed_jobs = options.get('get_indeed_jobs', False)
        self.get_zip_jobs = options.get('get_zip_jobs', False)
        self.get_all_jobs = options.get('get_all_jobs', False)

        self.existing_technologies = Technology.objects.values_list('name', flat=True)
        self.searchable_benefits = BenefitTitle.objects.filter(searchable=True).values_list('title', flat=True)
        self.companies = Company.objects.values('name', 'id')
        for company in self.companies:
            company['name'] = formatted_company_name(company['name'])

        if self.get_sof_jobs:
            self.get_all_sof_jobs()

        if self.get_dice_jobs:
            self.get_all_dice_jobs()

        if self.get_indeed_jobs:
            self.get_all_indeed_jobs()

        if self.get_zip_jobs:
            self.get_all_zip_jobs()

        if self.get_all_jobs:
            self.get_all_sof_jobs()
            self.get_all_dice_jobs()
            self.get_all_indeed_jobs()
            self.get_all_zip_jobs()

    def verbose_print(self, message):
        if self.verbose:
            print message

    def get_all_sof_jobs(self):
        self.verbose_print('Getting Stackoverflow Jobs')
        job_apis = ExternalJobListing.objects.filter(source='stackoverflow').values_list('api_link', flat=True)
        for state in US_STATES:
            self.verbose_print('Getting Stackoverflow Jobs in {}'.format(state))
            sof_rss_feed = 'http://stackoverflow.com/jobs/feed?location={}'.format(state)
            feed = feedparser.parse(sof_rss_feed)
            job_listings = []
            for job_listing in feed.entries:
                if not self.dry_run:
                    date_published = date_parser(job_listing['published'])
                    if job_listing['link'] in job_apis:
                        existing_job = ExternalJobListing.objects.filter(api_link=job_listing['link']).first()
                        self.handle_existing_job(existing_job)
                    else:
                        try:
                            job = ExternalJobListing.objects.create(
                                raw_company_name = job_listing['author'],
                                raw_location = job_listing['location'],
                                title = job_listing['title'],
                                description = job_listing['summary'],
                                api_link = job_listing['link'],
                                technologies = [tag['term'] for tag in job_listing.get('tags', [])],
                                date_published = date_published,
                                source = 'stackoverflow',
                                raw_job = job_listing,
                            )
                            matching_company = self.get_matching_company(job.raw_company_name)
                            if matching_company:
                                try:
                                    self.verbose_print('{} matched {}'.format(matching_company.name, job.raw_company_name))
                                except UnicodeEncodeError:
                                    pass
                                job.company = matching_company
                                job.save()
                                locations = CompanyLocation.objects.filter(company=job.company)
                                found_location = False
                                for location in locations:
                                    if location.raw_location == job.raw_location or \
                                        (location.city and location.city in job.raw_location):
                                        try:
                                            self.verbose_print('Found a matching location for job: {}'.format(location.city))
                                        except UnicodeEncodeError:
                                            pass
                                        found_location = True
                                        job.company_location = location
                                        job.enabled=True
                                        job.date_expired = job.date_published + timedelta(days=JOBS_EXPIRE_DAYS)
                                        job.save()
                                        self.add_tech_from_job_description(location, job.description)
                                        self.add_benefits_from_job_description(location, job.description)
                                        break
                                if not found_location:
                                    location = self.create_company_location(job.company, job_listing['location'])
                                    self.add_tech_from_job_description(location, job.description)
                                    self.add_benefits_from_job_description(location, job.description)

                        except KeyError:
                            self.verbose_print('KeyError on {}: Moving on'.format(job_listing['author']))
                            pass
        self.expire_old_jobs('stackoverflow')

    def get_all_dice_jobs(self):
        self.verbose_print('Getting Dice Jobs')
        dice_companies = DiceCompany.objects.all().order_by('-id')
        job_apis = ExternalJobListing.objects.filter(source='dice').values_list('api_link', flat=True)
        for dice_company in dice_companies:
            dice_id = dice_company.company_id
            dice_api_url = "http://service.dice.com/api/rest/jobsearch/v1/simple.json?diceid={}".format(dice_id)
            response = requests.get(dice_api_url)
            jobs_json = response.json()
            jobs_count = jobs_json.get('count', 0)
            jobs = jobs_json.get('resultItemList', [])
            for job_listing in jobs:
                if not self.dry_run:
                    date_published = date_parser(job_listing['date'] + ' 00:00:00 GMT')
                    if job_listing['detailUrl'] in job_apis:
                        existing_job = ExternalJobListing.objects.filter(api_link=job_listing['detailUrl']).first()
                        self.handle_existing_job(existing_job)
                    else:
                        job = ExternalJobListing.objects.create(
                            raw_company_name = job_listing['company'],
                            raw_location = job_listing['location'],
                            title = job_listing['jobTitle'],
                            description = job_listing['jobTitle'],
                            api_link = job_listing['detailUrl'],
                            date_published = date_published,
                            source = 'dice',
                            raw_job = job_listing,
                        )
                        matching_company = self.get_matching_company(job.raw_company_name)
                        if matching_company:
                            try:
                                self.verbose_print('{} matched {}'.format(matching_company.name, job.raw_company_name))
                            except UnicodeEncodeError:
                                pass
                            job.company = matching_company
                            job.save()
                            locations = CompanyLocation.objects.filter(company=job.company)
                            found_location = False
                            for location in locations:
                                if location.city and location.city in job.raw_location or \
                                    (location.city and location.city in job.raw_location):
                                    try:
                                        self.verbose_print('Found a matching city: {} for {}'.format(location.city, job.company.name))
                                    except UnicodeEncodeError:
                                        pass
                                    found_location = True
                                    job.company_location = location
                                    job.enabled=True
                                    job.date_expired = job.date_published + timedelta(days=JOBS_EXPIRE_DAYS)
                                    job.save()
                                    break
                            if not found_location:
                                location = self.create_company_location(job.company, job_listing['location'])

        self.expire_old_jobs('dice')


    def get_all_indeed_jobs(self):
        self.verbose_print('Getting Indeed Jobs')
        company_locations = CompanyLocation.objects.filter(enabled=True).order_by('-id')
        for company_location in company_locations:
            try:
                tech_list = company_location.technologies.values_list('name', flat=True)
                tech_list = ''.join(str(e)+" or " for e in tech_list)
                tech_list = tech_list.strip(' or ')
            except UnicodeEncodeError:
                self.verbose_print('Unicode error tech for Company Location: {}'.format(company_location.id))
                pass
            indeed_base_url = "http://api.indeed.com/ads/apisearch?publisher=4156503896322715&2&v=2&format=json&highlight=0"
            try:
                zipcode = "&l={zipcode}".format(zipcode=company_location.postal_code)
            except:
                continue
            radius = "&radius=50"
            limit = "&limit=1000"
            technologies = "&q=({tech_list})".format(tech_list=tech_list)
            try:
                company = ' company:"{company_name}"'.format(company_name=self.xstr(company_location.company.name))
            except:
                self.verbose_print('failed: {}'.format(company_location.company.id))
                continue
            query = technologies + company
            indeed_api_url = indeed_base_url + zipcode + radius + limit + query

            response = requests.get(indeed_api_url)
            jobs_json = response.content
            try:
                jobs = json.loads(jobs_json)
            except:
                self.verbose_print('Encountered an exception: Happens rarely, move on')
            for job_listing in jobs.get('results', []):
                if not self.dry_run:
                    if formatted_company_name(company_location.company.name) == formatted_company_name(job_listing['company']):
                        try:
                            self.verbose_print('{} matched {}'.format(company_location.company.name, job_listing['company']))
                        except UnicodeEncodeError:
                            pass
                        exists = ExternalJobListing.objects.filter(title = job_listing['jobtitle'], description=job_listing['snippet'])
                        date_published = date_parser(job_listing['date'])
                        if not exists:
                            job = ExternalJobListing.objects.create(
                                title = job_listing['jobtitle'],
                                description = job_listing['snippet'],
                                api_link = job_listing['url'],
                                source = 'indeed',
                                company_location = company_location,
                                company = company_location.company,
                                raw_job = job_listing,
                                date_published = date_published,
                                date_expired = date_published + timedelta(days=JOBS_EXPIRE_DAYS),
                                enabled=True,
                            )
                        else:
                            self.verbose_print('Duplicate JobListing: Marking as modified.')
                            self.update_existing_job(exists[0])
        self.expire_old_jobs('indeed')

    def get_all_zip_jobs(self):
        self.verbose_print('Getting ziprecruiter Jobs')

        tech_list = Technology.objects.filter(weight__gt=100).order_by('-weight').values_list('name', flat=True)[:20]
        tech_list = ''.join(str(e)+" " for e in tech_list)
        technologies = tech_list.strip()
        technologies = urllib.quote_plus(technologies)


        for state in US_STATES_FULL:
            page = 1
            results_per_page = 200
            previous_data = []

            zip_url = "https://api.ziprecruiter.com/jobs/v1?search={technologies}&location={location}&api_key={api_key}&jobs_per_page={results}&page={page}".format(api_key="bcnd6guk4qdwsdsxkp4yarmksdmzeucp", location=state, technologies=technologies, results=results_per_page, page=page)
            data = requests.get(zip_url).json()
            job_listings = []

            try:
                #the total_jobs reported are not always accurate so dont rely on that number
                num_pages = int(data['total_jobs'])/len(data['jobs'])
                self.verbose_print('\nFound {} jobs in {} state with {} pages of results.\n'.format(str(data['total_jobs']), state, num_pages))
            except ZeroDivisionError:
                num_pages = 0
                continue

            while page <= num_pages:
                if page > 1:
                    try:
                        if data['jobs'][0]['snippet'] == previous_data['jobs'][0]['snippet']:
                            self.verbose_print('\nPrevious data packet is the same as the new one. Skip the rest. Zip is bein dumb!!\n')
                            break
                    except IndexError:
                        #IndexError means Zip returned no jobs. Move on to the next state
                        break
                self.verbose_print('\nProcess {} jobs\n'.format(len(data['jobs'])))
                for job_listing in data['jobs']:
                    if not self.dry_run:
                        date_published = date_parser(job_listing['posted_time'] + ' GMT')
                        #Api urls are not unique so find a creative way to not get duplicates
                        existing_job = ExternalJobListing.objects.filter(
                                            title=job_listing['name'],
                                            description=job_listing['snippet'],
                                            source=job_listing['source'],
                                        ).first()
                        if existing_job:
                            self.handle_existing_job(existing_job)
                        else:
                            job = ExternalJobListing.objects.create(
                                raw_company_name = job_listing['hiring_company']['name'],
                                raw_location = job_listing['location'],
                                title = job_listing['name'],
                                description = job_listing['snippet'],
                                api_link = job_listing['url'],
                                date_published = date_published,
                                source = job_listing['source'],
                                aggregator_source = 'ziprecruiter',
                                raw_job = job_listing,
                            )
                            matching_company = self.get_matching_company(job.raw_company_name)
                            if matching_company:
                                try:
                                    self.verbose_print('{} matched {}'.format(matching_company.name, job.raw_company_name))
                                except UnicodeEncodeError:
                                    pass
                                job.company = matching_company
                                job.save()
                                locations = CompanyLocation.objects.filter(company=job.company)
                                found_location = False
                                for location in locations:
                                    if (location.city and location.city in job.raw_location) or \
                                        (location.raw_location in job.raw_location):
                                        try:
                                            self.verbose_print('Found a matching location: {} for {}'.format(location.raw_location, job.company.name))
                                        except UnicodeEncodeError:
                                            pass
                                        found_location = True
                                        job.company_location = location
                                        job.enabled=True
                                        job.date_expired = job.date_published + timedelta(days=JOBS_EXPIRE_DAYS)
                                        job.save()

                                        self.add_tech_from_job_description(location, job.description)
                                        self.add_benefits_from_job_description(location, job.description)
                                if not found_location:
                                    location = self.create_company_location(job.company, job_listing['location'])
                                    self.add_tech_from_job_description(location, job.description)
                                    self.add_benefits_from_job_description(location, job.description)
                page += 1
                self.verbose_print('\nPage number {}\n'.format(page))
                zip_url = "https://api.ziprecruiter.com/jobs/v1?search={technologies}&location={location}&api_key={api_key}&jobs_per_page={results}&page={page}".format(api_key="bcnd6guk4qdwsdsxkp4yarmksdmzeucp", location=state, technologies=technologies, results=results_per_page, page=page)
                previous_data = data
                data = requests.get(zip_url).json()

        self.expire_old_jobs('ziprecruiter')

    def format_job_description(self, job_description):
        job_description = job_description.replace('<b>', ' ').replace('</b>', ' ')
        job_description = job_description.replace(', ', ' ').replace('  ', ' ')
        job_description = job_description.replace('. ', ' ').replace('  ', ' ')
        return job_description.lower()

    def update_existing_job(self, job):
        job.date_modified = timezone.now()
        job.save()
        if not job.enabled:
            if not job.company:
                matching_company = self.get_matching_company(job.raw_company_name)
                if matching_company:
                    job.company = matching_company
                    job.save()
                    try:
                        self.verbose_print('{} matched {}'.format(matching_company.name, job.raw_company_name))
                    except UnicodeEncodeError:
                        pass
            if job.company:
                locations = CompanyLocation.objects.filter(company=job.company)
                found_location = False
                for location in locations:
                    if location.raw_location == job.raw_location or \
                        (location.city and location.city in job.raw_location):
                        found_location = True
                        self.verbose_print('Found a matching location for Existing listing: {}'.format(location.id))
                        job.company_location = location
                        job.enabled=True
                        job.save()
                        self.add_tech_from_job_description(location, job.description)
                        self.add_benefits_from_job_description(location, job.description)
                        break
            if job.company and not found_location and job.source != 'indeed':
                location = self.create_company_location(job.company, job.raw_location)
                self.add_tech_from_job_description(location, job.description)
                self.add_benefits_from_job_description(location, job.description)
        elif job.company_location and job.description:
            #Temporary Code till we have benefits and Technologies loaded
            self.add_tech_from_job_description(job.company_location, job.description)
            self.add_benefits_from_job_description(job.company_location, job.description)

    def expire_old_jobs(self, source):
        # A job that hasnt been modified/updated in 48 hours should be disabled
        self.verbose_print('Expiring old {} jobs'.format(source))
        if not self.dry_run:
            old_jobs_datetime = timezone.now() - timedelta(hours=24)
            jobs_cleanup = ExternalJobListing.objects.filter(
                                source=source,
                                enabled=True,
                                date_modified__lt=old_jobs_datetime
                            )
            self.verbose_print('Expiring {} {} jobs'.format(str(jobs_cleanup.count()), source))
            jobs_cleanup.update(enabled=False)

    def xstr(self, s):
        return '' if s is None else str(s)

    def get_matching_company(self, company_name):
        for company in self.companies:
            if company['name'] == formatted_company_name(company_name):
                return Company.objects.get(id=company['id'])
        return None

    def create_company_location(self, company, location):
        create_location = CompanyLocation.objects.create(
            company=company,
            raw_location=location,
            country='United States',
            enabled=False,
        )
        try:
            job_city = location.split(',')[0].strip()
            job_state = location.split(',')[1].strip()
            create_location.city = job_city
            create_location.state = job_state
            create_location.save()
        except:
            self.verbose_print('Could not get job or city. Move on.')
            pass
        return create_location

    def add_tech_from_job_description(self, location, job_description):
        try:
            self.verbose_print('Adding Technologies for location {}'.format(location))
            for technology in self.existing_technologies:
                formatted_technology = ' '+technology+' '
                formatted_description = self.format_job_description(job_description)
                if formatted_technology in formatted_description:
                    self.verbose_print('Added Technology: {} for location {}'.format(formatted_technology, location))
                    location.technologies.add(Technology.objects.get(name=technology))
            location.save()
        except UnicodeEncodeError:
            pass

    def add_benefits_from_job_description(self, location, job_description):
        try:
            self.verbose_print('Adding Benefits for location {}'.format(location))
            for benefit in self.searchable_benefits:
                formatted_benefit = ' '+benefit.lower()+' '
                formatted_description = self.format_job_description(job_description)
                if formatted_benefit in formatted_description:
                    self.verbose_print('Added Benefit: {} for location {}'.format(formatted_benefit, location))
                    benefit_title_object = BenefitTitle.objects.get(title=benefit)
                    benefit_object = Benefit.objects.filter(title=benefit_title_object).first()
                    if benefit_object is None:
                        Benefit.objects.create(
                            title=benefit_title_object,
                            company_location=location,
                            category=benefit_title_object.category,
                        )
                    else:
                        benefit_object.company_location=location
                        benefit_object.category=benefit_title_object.category
                        benefit_object.save()

        except UnicodeEncodeError:
            pass

    def handle_existing_job(self, existing_job):
        if existing_job:
            yesterday = timezone.now() - timedelta(days=1)
            if existing_job.date_modified.date() >= yesterday.date():
                self.verbose_print('Duplicate JobListing: Already marked as modified.')
            else:
                self.verbose_print('Duplicate JobListing: Marking as modified.')
                self.update_existing_job(existing_job)
        return
