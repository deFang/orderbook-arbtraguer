import collections
import logging
import time
# import traceback
from functools import wraps


def retry(
    max_retry_count=1,
    retry_interval_base=0.3,
    exceptions=(Exception),
    raise_exception=True,
    default_return_value=None,
):
    def decorator(func):
        @wraps(func)
        def newfn(*args, **kwargs):
            retry_count = 0
            backoff_base = 2
            backoff_max_value = 10
            while retry_count <= max_retry_count:
                try:
                    logging.debug(
                        f"{func} with retry: {retry_count}/{max_retry_count}"
                    )
                    return func(*args, **kwargs)

                except exceptions as ex:
                    # traceback.print_exc()
                    if retry_count == 0:
                        # logging.error(ex)
                        pass
                    else:
                        logging.info(
                            f"{func} error: attempt {retry_count}/{max_retry_count}"
                        )
                        if not raise_exception:
                            logging.exception(ex)
                    retry_count += 1

                sleep_in_seconds = retry_interval_base * backoff_base ** (
                    retry_count - 1
                )
                if sleep_in_seconds > backoff_max_value:
                    time.sleep(backoff_max_value)
                else:
                    time.sleep(sleep_in_seconds)

            if raise_exception:
                return func(*args, **kwargs)
            else:
                return default_return_value

        return newfn

    return decorator


# paged_since decorator
def paged_since(
    since_field_name="timestamp",
    paged_id_field_name="id",
    max_paged_count=1000,
    paged_interval=0.5,
    exceptions=(Exception),
):
    def decorator(func):
        @wraps(func)
        def newfn(*args, **kwargs):
            result = []
            since = kwargs.get("since")
            if since == None:
                raise Exception(f"{func} require 'since' key args")
            limit = kwargs.get("limit") or 1000

            kwargs.update({"limit": limit, "since": since})
            paged_count = 1
            result_dict = collections.OrderedDict()
            while True:
                try:
                    logging.debug(
                        f"{func} with paged_since: page={paged_count} limit={limit} since={since}"
                    )
                    res = func(*args, **kwargs)
                    if len(res) < limit:
                        result.extend(res)
                        break
                    else:
                        since = max([item[since_field_name] for item in res])
                        result.extend(res)
                        kwargs.update({"limit": limit, "since": since})

                except exceptions as ex:
                    # traceback.print_exc()
                    logging.error(ex)
                    break

                paged_count += 1
                if paged_count > max_paged_count:
                    logging.error(
                        f"{func}: reach the max_paged_count({max_paged_count})"
                    )
                    break
                logging.info(f"{func}: paged with index {paged_count}")
                time.sleep(paged_interval)

            for item in result:
                if item[paged_id_field_name] not in result_dict:
                    result_dict[item[paged_id_field_name]] = item
            return list(result_dict.values())

        return newfn

    return decorator
