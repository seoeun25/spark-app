package com.lezhin.wasp.util;

/**
 *
 * https://wiki.lezhin.com/display/BIZDEV/Infra-structure#Infra-structure-09.CloudSqlProxy
 *
 * @author seoeun
 * @since 2018.07.27
 */
public enum LezhinServerProp {

    PRODUCTION("production",
            "https://api.lezhin.com/api",
            "https://cms-dot-lezhincomix-api.appspot.com",
            "Bearer b58b583f-2bac-4035-a8c8-9a8c56e64085"),
    MIRROR("mirror",
            "https://mirror-www.lezhin.com/api",
            "https://cms-dot-lezhincomix-mirror.appspot.com",
            "Bearer c63a7469-08dc-41d9-b477-000e195cae87"),
    QA("qa",
            "https://q-api.lezhin.com/api",
            "https://cms-dot-lezhincomix-qa.appspot.com",
            "Bearer f632a8cc-65f0-48dd-bdae-a63ff2e0470e"),
    BETA("beta",
            "https://beta-api.lezhin.com/api",
            "https://cms-dot-lezhincomix-beta.appspot.com",
            "Bearer 4ce14c48-f0fa-46c9-87d7-a0e7af0aead0"),
    TEST("test", "https://localhost/api", "https://localhost/cms", ""),
    ;

    private String envName;
    private String apiUrl;
    private String cmsUrl;
    private String cmsToken;

    LezhinServerProp(String envName, String apiUrl, String cmsUrl, String cmsToken) {
        this.envName = envName;
        this.apiUrl = apiUrl;
        this.cmsUrl = cmsUrl;
        this.cmsToken = cmsToken;
    }

    public String getEnvName() {
        return envName;
    }

    public String getApiUrl() {
        return  apiUrl;
    }

    public String getCmsUrl() {
        return  cmsUrl;
    }

    public String getCmsToken() {
        return cmsToken;
    }

}
