package com.lezhin.wasp.util;

/**
 *
 * https://wiki.lezhin.com/display/BIZDEV/Infra-structure#Infra-structure-09.CloudSqlProxy
 *
 * @author seoeun
 * @since 2018.07.27
 */
public enum  DatabaseProp {

    PRODUCTION("production", "jdbc:mysql://127.0.0.1:3307/lezhincomics", "lezhin", "eoqkrfpwls$"),
    MIRROR("mirror", "jdbc:mysql://127.0.0.1:3309/lezhincomics_mirror", "lezhin", "eoqkrfpwls$"),
    QA("qa", "jdbc:mysql://127.0.0.1:3309/lezhincomics_qa", "lezhin", "eoqkrfpwls$"),
    BETA("beta", "jdbc:mysql://127.0.0.1:3309/lezhincomics", "lezhin", "eoqkrfpwls$"),
    PRD_LOG01("prd_log01", "jdbc:mysql://127.0.0.1:3308/lezhincomics", "lezhin", "eoqkrfpwls$"),
    TEST("test", "jdbc:mysql://127.0.0.1:3306/lezhincomics", "root", ""),
    ;

    private String envName;
    private String url;
    private String user;
    private String password;

    DatabaseProp(String envName, String url, String user, String password) {
        this.envName = envName;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public String getEnvName() {
        return envName;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
