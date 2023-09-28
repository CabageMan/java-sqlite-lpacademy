package com.ironhead;

import com.ironhead.model.Album;
import com.ironhead.model.Artist;
import com.ironhead.model.Datasource;
import com.ironhead.model.SongArtist;

import java.util.List;

public class Main {
  public static void main(String[] args) {
    Datasource datasource = new Datasource();

    if (!datasource.open()) {
      System.out.println("Couldn't open datasource");
      return;
    }

    // Artists
    List<Artist> artists = datasource.queryArtists();
    if (artists == null) {
      System.out.println("There are no artists");
      return;
    }

    for (Artist artist: artists) {
      System.out.println("Artist ID: " + artist.getId() + ", Artist Name: " + artist.getName());
    }

    // Albums
    List<Album> albums = datasource.queryAlbums(Datasource.SortOrder.BY_ASC);
    if (albums == null) {
      System.out.println("There are no albums");
      return;
    }

    for (Album album: albums) {
      System.out.println( "Album ID: " + album.getId() +
              ", Album Name: " + album.getName() +
              ", Album Artist: " + album.getArtistId());
    }

    List<String> albumsNamesForArtist = datasource.queryAlbumsForArtist("Iron Maiden", Datasource.SortOrder.BY_DESC);
    for (String albumName : albumsNamesForArtist) {
      System.out.println("Name: " + albumName);
    }

    List<SongArtist> songArtists = datasource.queryArtistsForSong("Go Your Own Way", Datasource.SortOrder.BY_ASC);
    if (songArtists == null) {
      System.out.println("There are no songs artists");
      return;
    }
    for (SongArtist songArtist : songArtists) {
      System.out.println("Artist name: " + songArtist.getArtistName() + " | " +
              " Album name: " + songArtist.getAlbumName() + " | " +
              " Song track: " + songArtist.getSongTrack());
    }

    datasource.querySongsMetadata();

    datasource.close();
  }
}