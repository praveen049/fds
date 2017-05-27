from aggregator import Aggregator

if __name__ == "__main__":
    agg = Aggregator('dis','hourly','first','name,values')

    print agg

    agg.setconf('api_handler')

    sp_df = agg.read_data()

    print agg.get_pandas(sp_df)

