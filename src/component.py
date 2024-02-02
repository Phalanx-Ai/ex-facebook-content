import csv
import logging
import facebook
import sys

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PAGE_ID = 'page_id'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_TOKEN, KEY_PAGE_ID]
REQUIRED_IMAGE_PARS = []

FILENAME_POSTS = "posts.csv"
FILENAME_COMMENTS = "comments.csv"

CSV_FIELDS = [
    "id",
    "image_url",
    "title",
    "sentiment",
    "react_haha",
    "react_anger",
    "parent_id",
    "resource",
    "react_share",
    "content",
    "react_sorry",
    "language",
    "author",
    "url",
    "source",
    "react_wow",
    "react_like",
    "react_love",
    "published_at",
    "in_reply_to"
]


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def _get_page_name(self):
        return (
            facebook.GraphAPI(
                access_token=self.params[KEY_API_TOKEN]
            ).get_object(
                "%s" % (self.params[KEY_PAGE_ID])
            )
        )['name']

    def _get_posts(self):
        # @todo: Is paging relevant for us?
        return (
            facebook.GraphAPI(
                access_token=self.params[KEY_API_TOKEN]
            ).get_object(
                '%s/posts' % (self.params[KEY_PAGE_ID]),
                fields=(
                    "id,created_time,message,permalink_url,"
                    "insights.metric(post_reactions_by_type_total).period(lifetime)"
                    ".as(post_reactions_by_type_total),shares,full_picture"
                )
            )
        )['data']

    def _transform_post(self, posts):
        sposts = []
        for post in posts:
            spost = {}
            spost['id'] = post['id']
            spost['source'] = 'facebook'
            spost['resource'] = self.page_name
            spost['url'] = post['permalink_url']
            spost['content'] = post.get('message', '')
            spost['published_at'] = post['created_time'][:-5] + 'Z'
            spost['author'] = self.page_name
            spost['image_url'] = post.get('full_picture', '')

            spost['title'] = None
            spost['parent_id'] = None
            spost['language'] = "missing"
            spost['sentiment'] = "missing"

            spost['react_share'] = post.get('shares', {'count': 0})['count']
            if 'post_reactions_by_type_total' in post:
                for reaction in ['like', 'love', 'wow', 'haha', 'sorry', 'anger']:
                    spost['react_%s' % (reaction)] = \
                        post['post_reactions_by_type_total']['data'][0]['values'][0]['value'].get(reaction, 0)

            sposts.append(spost)

        return sposts

    def _get_comments(self, posts):
        graph = facebook.GraphAPI(access_token=self.params[KEY_API_TOKEN])
        comments = []
        for post in posts:
            for comment in graph.get_all_connections(
                post['id'],
                'comments',
                filter='stream',
                fields='id,created_time,permalink_url,from,parent{id},message,like_count',
                order='reverse_chronological'
            ):
                scomment = {}
                scomment['id'] = "%s_%s" % (self.params[KEY_PAGE_ID], comment['id'])
                scomment['source'] = 'facebook'
                scomment['resource'] = self.page_name
                scomment['url'] = comment['permalink_url']
                scomment['content'] = comment['message']
                scomment['react_like'] = comment['like_count']
                scomment['published_at'] = comment['created_time'][:-5] + 'Z'
                scomment['author'] = comment['from']['name'] if 'from' in comment else 'N/A'

                if 'parent' in comment:
                    scomment['in_reply_to'] = "%s_%s" % (self.params[KEY_PAGE_ID], comment['parent']['id'])
                else:
                    scomment['in_reply_to'] = post['id']

                scomment['language'] = "missing"
                scomment['sentiment'] = "missing"

                comments.append(scomment)

        return comments

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        self.params = self.configuration.parameters

        table_posts = self.create_out_table_definition(FILENAME_POSTS, incremental=True, primary_key=['id'])
        table_comments = self.create_out_table_definition(FILENAME_COMMENTS, incremental=True, primary_key=['id'])

        try:
            self.page_name = self._get_page_name()

            posts = self._transform_post(self._get_posts())
            comments = self._get_comments(posts)
        except facebook.GraphAPIError as error:
            print(error)
            sys.exit(1)

        with open(table_posts.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(posts)
        self.write_manifest(table_posts)

        with open(table_comments.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(comments)
        self.write_manifest(table_comments)


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
