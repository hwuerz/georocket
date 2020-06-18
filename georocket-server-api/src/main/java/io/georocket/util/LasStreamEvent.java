package io.georocket.util;

/**
 * An event produced during LAS parsing
 * @author Hendrik M. Wuerz
 */
public class LasStreamEvent extends StreamEvent {

  private final Lasinfo lasinfo;

  /**
   * Constructs a new event
   * @param lasinfo The information extracted from the las file.
   */
  public LasStreamEvent(Lasinfo lasinfo) {
    super(0);
    this.lasinfo = lasinfo;
  }


  /**
   * @return The information extracted from the las file.
   */
  public Lasinfo getEvent() {
    return lasinfo;
  }

}
