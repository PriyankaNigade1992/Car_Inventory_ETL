import src.provider_feed_reader as feed_reader


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('ETL Process Started')
    feed_reader.SchemaReader().read_feed_data()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
