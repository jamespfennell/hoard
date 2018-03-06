def unittest_feed_reader(file_path):
    """Check if a unittest feed is valid and, if so, return its timestamp.

    To be implemented!
    """
    with open(file_path, 'r') as f:
        content = f.read()
    return int(content)


def _analyze_unittest_feeds(iterator):
    """Return statistics on the number of valid/empty/corrupt unittest feeds.
    """
    feed_data_by_feed_url = {}
    for (feed, timestamp, file_path) in iterator:
        (feed_id, feed_url, _, _) = feed

        if feed_url not in feed_data_by_feed_url:
            feed_data_by_feed_url[feed_url] = {
                'valid': 0,
                'corrupt': 0,
                'empty': 0
                }

        with open(file_path, 'r') as f:
            content = f.read()
        if content == '':
            feed_data_by_feed_url[feed_url]['empty'] += 1
        else:
            try:
                int(content)
                feed_data_by_feed_url[feed_url]['valid'] += 1
            except ValueError:
                feed_data_by_feed_url[feed_url]['corrupt'] += 1

    return feed_data_by_feed_url


def _pass_or_fail(condition):
    """Print whether a condition is true."""
    if condition:
        print('  {}'.format(_green('Passed')))
    else:
        print('  {}'.format(_red('Failed')))


def _green(s):
    return '\033[92m\033[01m{}\033[0m'.format(s)


def _red(s):
    return '\033[91m\033[01m{}\033[0m'.format(s)


def _compare(row, field, a, b, c):
    if a is None:
        a = ''
        test = (b == c)
    elif b is None:
        b = ''
        test = (a == c)
    elif c is None:
        c = ''
        test = (a == b)
    else:
        test = (a == b) and (b == c)
    print(row.format(field, a, b, c))
    return test
