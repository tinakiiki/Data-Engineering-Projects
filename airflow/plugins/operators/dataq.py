for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
 
            records = redshift.get_records(sql)[0]
 
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
 
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
In the DAG, the tests can be specified as below:

    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}]
