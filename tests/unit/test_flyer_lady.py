from datetime import datetime, timezone
from flyer_lady.models import Special
from flyer_lady.platforms.facebook_story_publisher import FacebookStoryPublisher
from flyer_lady.platforms.facebook_feed_publisher import FacebookFeedPublisher
class FakeGraph:
    def __init__(self): self.calls=[]; self.client=self
    def upload_unpublished_photo(self,page_id,token,media_url): self.calls.append(("upload",page_id,token,media_url)); return {"id":"photo-1"}
    def publish_photo_story(self,page_id,token,photo_id): self.calls.append(("story",page_id,token,photo_id)); return {"post_id":"story-1"}
    def publish_feed_photo(self,page_id,token,media_url,caption): self.calls.append(("feed",page_id,token,media_url,caption)); return {"id":"feed-1"}
    def post_with_token(self,token,path,data=None,**kwargs): self.calls.append(("post",token,path,data)); return {"id":"feed-1"}
def make_special(): return Special(id=10,location_id=1,created_by="tester",text="10% off servicing",media_url="https://example.com/s.jpg",booking_link="/book/a/b",status="approved",created_at=datetime.now(timezone.utc),updated_at=datetime.now(timezone.utc))
def test_facebook_story_two_step():
    g=FakeGraph(); assert FacebookStoryPublisher(g).publish(make_special(),"page-1","SECRET")=="story-1"; assert g.calls[0][0]=="upload"; assert g.calls[1]==("story","page-1","SECRET","photo-1")
def test_facebook_feed_calls_publish(monkeypatch):
    monkeypatch.setenv("PHANTA_PUBLIC_BASE_URL", "https://app.example.test")
    g=FakeGraph(); assert FacebookFeedPublisher(g).publish(make_special(),"page-1","SECRET")=="feed-1"; assert "Book here" in g.calls[0][4]
