package storm.experiments.model;

import java.io.Serializable;

public class VisitorsFact implements Serializable {
    private static final long serialVersionUID = 1L;

    private String country;
    private String url;
    private int visitors;

    public VisitorsFact(String country, String url, int visitors) {
        this.country = country;
        this.visitors = visitors;
        this.url = url;
    }

    public String getCountry() {
        return country;
    }

    public String getUrl() {
        return url;
    }

    public int getVisitors() {
        return visitors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VisitorsFact that = (VisitorsFact) o;

        if (visitors != that.visitors) return false;
        if (country != null ? !country.equals(that.country) : that.country != null) return false;
        if (url != null ? !url.equals(that.url) : that.url != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + (url != null ? url.hashCode() : 0);
        result = 31 * result + visitors;
        return result;
    }
}