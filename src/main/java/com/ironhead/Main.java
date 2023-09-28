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

    printDivider();

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

    printDivider();

    List<String> albumsNamesForArtist = datasource.queryAlbumsForArtist("Iron Maiden", Datasource.SortOrder.BY_DESC);
    for (String albumName : albumsNamesForArtist) {
      System.out.println("Album Name: " + albumName);
    }

    printDivider();

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

    printDivider();

    datasource.querySongsMetadata();

    printDivider();

    System.out.println("Number of songs: " + datasource.getCount(Datasource.TABLE_SONGS));
    System.out.println("Number of albums: " + datasource.getCount(Datasource.TABLE_ALBUMS));
    System.out.println("Number of artists: " + datasource.getCount(Datasource.TABLE_ARTISTS));

    printDivider();

    if (datasource.createViewForSongArtists()) {
      System.out.println("Song Artist View Created Successfully");
    } else {
      System.out.println("Song Artist View Creation failed");
    }

    printDivider();

    List<SongArtist> viewSongInfo = datasource.queryViewSongInfo("Go Your Own Way");
    if (viewSongInfo == null) {
      System.out.println("Failed to load song info");
      return;
    } else if (viewSongInfo.isEmpty()) {
      System.out.println("There is no song info");
      return;
    }
    for (SongArtist songArtist : viewSongInfo) {
      System.out.println("From View:\nArtist Name: " + songArtist.getArtistName() + " | " +
              "Album Name: " + songArtist.getAlbumName() + " | " +
              "Song Track: " + songArtist.getSongTrack()
      );
    }

    datasource.close();
  }

  private static void printDivider() {
    System.out.println("-----------------------------------------------------");
  }
}