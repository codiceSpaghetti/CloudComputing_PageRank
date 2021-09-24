package it.unipi.cc.pagerank.hadoop.serialize;

import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Page implements WritableComparable<Page> {
    private String title;
    private double rank;

    //-------------------------------------------------------------------------------

    public Page() { }

    public Page(final String title, final double rank) {
        set(title, rank);
    }

    //-------------------------------------------------------------------------------

    public void setTitle(final String title) { this.title = title; }

    public void setRank(final double rank) { this.rank = rank; }

    public void set(final String title, final double rank) {
        setTitle(title);
        setRank(rank);
    }

    public void setFromJson(final String json) {
        Page fromJson = new Gson().fromJson(json, Page.class);
        set(fromJson.getTitle(), fromJson.getRank());
    }

    public String getTitle() { return this.title; }

    public double getRank() { return this.rank; }

    //-------------------------------------------------------------------------------

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.title);
        out.writeDouble(this.rank);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.title = in.readUTF();
        this.rank = in.readDouble();
    }

    //-------------------------------------------------------------------------------

    public String toHumanString() { return "[Title: " + title + "]\t" + rank; }

    @Override
    public String toString() {
        String json = new Gson().toJson(this);
        return json;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Page)) {
            return false;
        }

        Page that = (Page) o;
        return that.getTitle().equals(this.title)
                && this.rank == that.getRank();
    }

    @Override
    public int hashCode() { return this.title.hashCode(); }

    @Override
    public int compareTo(Page that) {
        double thatRank = that.getRank();
        String thatTitle = that.getTitle();
        return this.rank < thatRank ? 1 : (this.rank == thatRank ? this.title.compareTo(thatTitle) : -1);
    }
}
