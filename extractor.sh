rm ./res/*

read -n 1 -p "Do you want to extract facebook_post? [y/n] " -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
   python3 extractor.py --file_prefix facebook_post --key id-message-from.id-from.name-created_time
fi

read -n 1 -p "Do you want to extract instagram_post? [y/n] " -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
   python3 extractor.py --file_prefix instagram_post --key id-caption.text-user.id-user.username-created_time
fi

read -n 1 -p "Do you want to extract twitter_status? [y/n] " -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
   python3 extractor.py --file_prefix twitter_status --key id-full_text-user.id-user.screen_name-created_at
fi

read -n 1 -p "Do you want to extract youtube_video? [y/n] " -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
   python3 extractor.py --file_prefix youtube_video --key id-snippet.title-snippet.channelId-snippet.channelTitle-snippet.publishedAt
fi

